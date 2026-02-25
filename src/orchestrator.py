import asyncio
import logging
from datetime import datetime

from src.models import Business, QualifiedLead, QualificationResult
from src.scraper import scrape_google_maps
from src.qualifier import qualify_businesses
from src.dedup import DeduplicationEngine
from src.sheets import SheetsManager
from src.emailer import EmailSender
from src.utils import load_config, get_daily_rotation, find_country_for_city

logger = logging.getLogger("leadgen.orchestrator")


CITY_COUNTRY_MAP = {
    "bangkok": "Thailand",
    "new york": "USA", "houston": "USA", "phoenix": "USA",
    "san francisco": "USA", "seattle": "USA", "denver": "USA",
    "atlanta": "USA", "boston": "USA", "san diego": "USA",
    "calgary": "Canada", "montreal": "Canada", "ottawa": "Canada",
    "birmingham": "UK", "leeds": "UK", "liverpool": "UK",
    "edinburgh": "UK", "glasgow": "UK", "bristol": "UK",
    "gold coast": "Australia", "canberra": "Australia",
    "auckland": "New Zealand", "wellington": "New Zealand",
    "christchurch": "New Zealand",
    "sharjah": "UAE", "ajman": "UAE",
    "mumbai": "India", "delhi": "India", "bangalore": "India",
    "singapore": "Singapore",
    "kuala lumpur": "Malaysia",
    "jakarta": "Indonesia",
    "tokyo": "Japan",
    "seoul": "South Korea",
    "hong kong": "Hong Kong",
}


def _guess_country(city: str) -> str:
    """Best-effort country guess for cities not in the config."""
    return CITY_COUNTRY_MAP.get(city.lower().strip(), "Unknown")


def _build_lead(biz: Business, result: QualificationResult) -> QualifiedLead:
    return QualifiedLead(
        name=biz.name,
        address=biz.address,
        city=biz.city,
        country=biz.country,
        niche=biz.niche,
        phone=biz.phone,
        website=biz.website,
        rating=biz.rating,
        review_count=biz.review_count,
        contact_email=result.contact_email,
        qualification_reasons=", ".join(result.reasons),
        ai_score=result.ai_score,
        ai_design_score=result.ai_design_score,
        ai_mobile_score=result.ai_mobile_score,
        ai_professionalism_score=result.ai_professionalism_score,
        ai_cta_score=result.ai_cta_score,
        ai_summary=result.ai_summary or "",
        ai_issues=result.ai_issues or [],
    )


def _worksheet_title_for_today() -> str:
    return datetime.now().strftime("%b %d")


async def run_pipeline(
    city: str | None = None,
    country: str | None = None,
    niche: str | None = None,
    headless: bool = True,
    send_emails: bool = True,
    use_ai: bool = False,
    config_path: str = "config/settings.yaml",
) -> dict:
    """Run the full scrape -> qualify -> dedup -> sheet -> email pipeline."""
    config = load_config(config_path)
    scraper_config = config.get("scraper", {})
    qualifier_config = config.get("qualifier", {})
    email_config = config.get("email", {})

    stats = {
        "scraped": 0,
        "qualified": 0,
        "duplicates_removed": 0,
        "added_to_sheet": 0,
        "emails_sent": 0,
        "errors": [],
    }

    if city and not country:
        detected = find_country_for_city(city, config)
        if detected:
            country = detected
        else:
            logger.warning(
                f"City '{city}' not found in config. "
                f"Use --country to specify the country."
            )
            country = _guess_country(city)

    if city and niche:
        targets = [{"city": city, "country": country, "niche": niche}]
    elif city:
        niches = config.get("niche_priority", config["niches"])[:3]
        targets = [{"city": city, "country": country, "niche": n} for n in niches]
    else:
        rotation = get_daily_rotation(config)
        niches = config.get("niche_priority", config["niches"])[:3]
        targets = []
        for loc in rotation:
            for n in niches:
                targets.append({"city": loc["city"], "country": loc["country"], "niche": n})

    logger.info(f"Pipeline targets: {len(targets)} city+niche combinations")
    for t in targets:
        logger.info(f"  - {t['niche']} in {t['city']}, {t['country']}")

    # Step 1: Scrape
    all_businesses: list[Business] = []
    for target in targets:
        logger.info(f"\n{'='*60}")
        logger.info(f"Scraping: {target['niche']} in {target['city']}")
        logger.info(f"{'='*60}")

        businesses = await scrape_google_maps(
            niche=target["niche"],
            city=target["city"],
            country=target["country"],
            max_results=scraper_config.get("max_results_per_search", 120),
            headless=headless,
        )
        all_businesses.extend(businesses)
        stats["scraped"] += len(businesses)

    if not all_businesses:
        logger.warning("No businesses scraped. Pipeline complete.")
        return stats

    logger.info(f"\nTotal scraped: {stats['scraped']} businesses")

    # Step 2: Qualify
    logger.info(f"\n{'='*60}")
    logger.info("Qualifying businesses...")
    logger.info(f"{'='*60}")

    qualified_pairs = await qualify_businesses(
        all_businesses,
        max_concurrent=qualifier_config.get("max_concurrent_checks", 5),
        use_ai=use_ai,
        timeout=qualifier_config.get("load_timeout_seconds", 15),
        slow_threshold=qualifier_config.get("slow_threshold_seconds", 4.0),
        outdated_before=qualifier_config.get("outdated_copyright_before", 2020),
    )

    qualified_businesses = [
        (biz, result)
        for biz, result in qualified_pairs
        if result.qualifies
    ]
    stats["qualified"] = len(qualified_businesses)
    logger.info(f"Qualified: {stats['qualified']}/{stats['scraped']}")

    if not qualified_businesses:
        logger.warning("No businesses qualified. Pipeline complete.")
        return stats

    # Step 3: Dedup
    logger.info(f"\n{'='*60}")
    logger.info("Deduplicating against tracker...")
    logger.info(f"{'='*60}")

    sheets = SheetsManager()
    dedup = DeduplicationEngine()

    try:
        existing = sheets.get_all_leads_all_sheets()
        dedup.load_existing_leads(existing)
    except Exception as e:
        logger.warning(f"Could not load existing leads for dedup: {e}")
        logger.info("Proceeding without dedup against existing tracker")

    unique_pairs = []
    for biz, result in qualified_businesses:
        if not dedup.is_duplicate(biz):
            unique_pairs.append((biz, result))
            dedup.register(biz)

    stats["duplicates_removed"] = len(qualified_businesses) - len(unique_pairs)
    logger.info(f"After dedup: {len(unique_pairs)} unique leads (removed {stats['duplicates_removed']} duplicates)")

    # Step 4: Write to Google Sheets
    logger.info(f"\n{'='*60}")
    logger.info("Writing leads to Google Sheets...")
    logger.info(f"{'='*60}")

    leads = [_build_lead(biz, result) for biz, result in unique_pairs]
    ws_title = _worksheet_title_for_today()

    try:
        added = sheets.append_leads(ws_title, leads)
        stats["added_to_sheet"] = added
        logger.info(f"Added {added} leads to worksheet '{ws_title}'")
    except Exception as e:
        stats["errors"].append(f"Sheets write error: {e}")
        logger.error(f"Failed to write to Google Sheets: {e}")

    # Step 5: Send emails
    if send_emails:
        logger.info(f"\n{'='*60}")
        logger.info("Sending outreach emails...")
        logger.info(f"{'='*60}")

        emailer = EmailSender(
            template_path=email_config.get("template_file", "templates/outreach_email.txt"),
            daily_limit=email_config.get("daily_limit", 80),
            delay_min=email_config.get("delay_between_sends_min", 30),
            delay_max=email_config.get("delay_between_sends_max", 60),
        )

        email_leads = []
        for lead in leads:
            if lead.contact_email:
                email_leads.append({
                    "email": lead.contact_email,
                    "business_name": lead.name,
                    "niche": lead.niche,
                    "website": lead.website or "",
                    "city": lead.city,
                    "qualification_reasons": lead.qualification_reasons,
                    "ai_score": lead.ai_score,
                    "ai_design_score": lead.ai_design_score,
                    "ai_mobile_score": lead.ai_mobile_score,
                    "ai_professionalism_score": lead.ai_professionalism_score,
                    "ai_cta_score": lead.ai_cta_score,
                    "ai_summary": lead.ai_summary,
                    "ai_issues": lead.ai_issues,
                })

        if email_leads:
            results = await emailer.send_batch(email_leads)
            stats["emails_sent"] = sum(1 for r in results if "error" not in r)
            logger.info(f"Emails sent: {stats['emails_sent']}/{len(email_leads)}")
        else:
            logger.info("No leads with contact emails to send to")

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("PIPELINE COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"  Scraped:     {stats['scraped']}")
    logger.info(f"  Qualified:   {stats['qualified']}")
    logger.info(f"  Duplicates:  {stats['duplicates_removed']}")
    logger.info(f"  Added:       {stats['added_to_sheet']}")
    logger.info(f"  Emailed:     {stats['emails_sent']}")
    if stats["errors"]:
        logger.warning(f"  Errors:      {len(stats['errors'])}")
        for err in stats["errors"]:
            logger.warning(f"    - {err}")

    return stats


async def run_email_only(
    worksheet_title: str | None = None,
    config_path: str = "config/settings.yaml",
) -> dict:
    """Send emails to un-emailed leads in a worksheet."""
    config = load_config(config_path)
    email_config = config.get("email", {})

    ws_title = worksheet_title or _worksheet_title_for_today()
    sheets = SheetsManager()
    unsent = sheets.get_unsent_leads(ws_title)

    if not unsent:
        logger.info(f"No unsent leads in '{ws_title}'")
        return {"emails_sent": 0}

    emailer = EmailSender(
        template_path=email_config.get("template_file", "templates/outreach_email.txt"),
        daily_limit=email_config.get("daily_limit", 80),
        delay_min=email_config.get("delay_between_sends_min", 30),
        delay_max=email_config.get("delay_between_sends_max", 60),
    )

    sent_count = 0
    for row_idx, record in unsent:
        email = record.get("Contact Email", "")
        if not email or "@" not in email:
            continue

        ai_summary_raw = record.get("AI Summary", "")
        ai_score_raw = record.get("AI Score", "")
        ai_score_int = None
        if ai_score_raw:
            try:
                ai_score_int = int(ai_score_raw.replace("/10", "").strip())
            except ValueError:
                pass

        result = await emailer.send_email(
            to_email=email,
            business_name=record.get("Business Name", ""),
            niche=record.get("Niche", ""),
            website=record.get("Website", ""),
            city=record.get("City", ""),
            qualification_reasons=record.get("Qualification Reasons", ""),
            ai_score=ai_score_int,
            ai_summary=ai_summary_raw,
        )

        if "error" not in result:
            sent_at = datetime.now().isoformat()
            sheets.mark_email_sent(ws_title, row_idx, sent_at)
            sent_count += 1

        if emailer.remaining_today <= 0:
            break

        await asyncio.sleep(
            __import__("random").randint(
                email_config.get("delay_between_sends_min", 30),
                email_config.get("delay_between_sends_max", 60),
            )
        )

    logger.info(f"Email-only run complete: {sent_count} emails sent")
    return {"emails_sent": sent_count}


def get_status(config_path: str = "config/settings.yaml") -> dict:
    """Get current pipeline status and daily progress."""
    config = load_config(config_path)
    rotation = get_daily_rotation(config)
    ws_title = _worksheet_title_for_today()

    status = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "worksheet": ws_title,
        "rotation_cities": rotation,
        "priority_niches": config.get("niche_priority", config["niches"])[:3],
    }

    try:
        sheets = SheetsManager()
        stats = sheets.get_daily_stats(ws_title)
        status.update(stats)
    except Exception as e:
        status["sheet_error"] = str(e)

    return status
