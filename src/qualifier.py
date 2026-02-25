import asyncio
import logging
import re
import time
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from src.models import Business, QualificationResult

logger = logging.getLogger("leadgen.qualifier")

SOCIAL_DOMAINS = {
    "facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "linkedin.com", "youtube.com", "yelp.com",
    "m.facebook.com", "www.facebook.com", "www.instagram.com",
    "www.tiktok.com", "www.linkedin.com", "www.youtube.com",
    "www.yelp.com", "www.x.com",
}

FREE_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "yahoo.co.uk", "hotmail.com", "outlook.com",
    "aol.com", "icloud.com", "mail.com", "protonmail.com", "ymail.com",
    "live.com", "msn.com", "comcast.net", "att.net", "verizon.net",
}

CHAIN_INDICATORS = [
    "franchise opportunities", "become a franchisee", "franchise info",
    "nationwide locations", "locations across",
    r"over \d+ locations", r"\d{3,}\+ locations",
    "find a location near you", "store locator",
    "corporate headquarters", "corporate office",
]

ECOMMERCE_INDICATORS = [
    "add to cart", "shopping cart", "shopify", "woocommerce",
    "bigcommerce", "magento", "add to bag", "view cart",
    "my cart", "cart total",
]

OUTDATED_PATTERNS = [
    r"<font\s", r"<center>", r"<marquee",
    r"<blink", r"<frameset", r"<frame\s", r"\.swf",
    r"best viewed in", r"optimized for internet explorer",
    r"<table[^>]*bgcolor", r"<body[^>]*bgcolor",
    r"<img[^>]*border\s*=\s*[\"']?\d", r"<hr[^>]*noshade",
]

MOBILE_VIEWPORT_PATTERNS = [
    re.compile(r'<meta[^>]*name=["\']viewport["\'][^>]*content=["\'][^"\']*width=device-width', re.I),
    re.compile(r'<meta[^>]*content=["\'][^"\']*width=device-width[^>]*name=["\']viewport["\']', re.I),
    re.compile(r'@media\s*\(', re.I),
    re.compile(r'@media\s+screen\s+and\s*\(\s*max-width', re.I),
]


def _is_mobile_friendly(html: str) -> bool:
    """Check if a page has mobile viewport or responsive CSS."""
    for pattern in MOBILE_VIEWPORT_PATTERNS:
        if pattern.search(html):
            return True
    return False

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
)

COPYRIGHT_YEAR_PATTERN = re.compile(
    r'(?:©|&copy;|copyright)\s*(?:\d{4}\s*[-–]\s*)?(\d{4})',
    re.I,
)


async def qualify_business(
    business: Business,
    timeout: float = 15.0,
    slow_threshold: float = 4.0,
    outdated_before: int = 2020,
) -> QualificationResult:
    """Analyze a business to determine if it qualifies as a lead."""
    result = QualificationResult()

    if not business.website:
        result.qualifies = True
        result.reasons.append("No website")
        result.has_website = False
        return result

    url = business.website
    if not url.startswith("http"):
        url = "https://" + url

    parsed = urlparse(url)
    domain = parsed.netloc.lower().lstrip("www.")

    # Check if the website is just a social media page (exact domain match)
    if domain in SOCIAL_DOMAINS or any(domain.endswith("." + social) for social in SOCIAL_DOMAINS):
        result.qualifies = True
        result.is_social_only = True
        result.reasons.append(f"Social media only ({domain})")
        return result

    try:
        html, load_time, status_code = await _fetch_page(url, timeout)
    except Exception as e:
        result.qualifies = True
        result.has_broken_layout = True
        result.reasons.append(f"Website unreachable: {e}")
        return result

    result.has_website = True
    result.load_time_seconds = load_time

    if status_code and status_code >= 400:
        # 404/410 = truly broken, 500+ = server error -- these qualify
        # 403 should already have been retried via browser fallback,
        # so if we still get it, the site is genuinely blocking everyone
        if status_code in (404, 410):
            result.qualifies = True
            result.has_broken_layout = True
            result.reasons.append(f"Website broken (HTTP {status_code})")
            return result
        elif status_code >= 500:
            result.qualifies = True
            result.has_broken_layout = True
            result.reasons.append(f"Website server error (HTTP {status_code})")
            return result
        # For other 4xx codes after browser retry, skip -- likely captcha wall
        # that real users can pass, so the site probably works fine
        elif not html or len(html) < 500:
            result.qualifies = False
            result.disqualify_reason = f"Bot-protected site (HTTP {status_code}), likely functional"
            return result

    if not html:
        result.qualifies = True
        result.has_broken_layout = True
        result.reasons.append("Empty website response")
        return result

    soup = BeautifulSoup(html, "html.parser")
    html_lower = html.lower()

    if _is_chain_or_franchise(html_lower, soup):
        result.is_chain_or_franchise = True
        result.disqualify_reason = "Large chain or franchise"
        result.qualifies = False
        return result

    if _is_ecommerce(html_lower, soup):
        result.disqualify_reason = "E-commerce store"
        result.qualifies = False
        return result

    if load_time > slow_threshold:
        result.is_slow = True
        result.reasons.append(f"Slow loading ({load_time:.1f}s)")

    if not _is_mobile_friendly(html):
        result.is_mobile_friendly = False
        result.reasons.append("Not mobile-friendly (no viewport meta)")

    if _has_outdated_patterns(html_lower):
        result.is_outdated = True
        result.reasons.append("Outdated design patterns detected")

    copyright_year = _get_copyright_year(html)
    if copyright_year and copyright_year < outdated_before:
        result.is_outdated = True
        result.reasons.append(f"Outdated copyright year ({copyright_year})")

    emails = EMAIL_PATTERN.findall(html)
    contact_email = _find_contact_email(emails)
    if contact_email:
        result.contact_email = contact_email
        email_domain = contact_email.split("@")[1].lower()
        if email_domain in FREE_EMAIL_DOMAINS:
            result.uses_free_email = True
            result.reasons.append(f"Uses free email ({email_domain})")

    if _has_broken_layout(soup, html_lower):
        result.has_broken_layout = True
        result.reasons.append("Poor design / broken layout indicators")

    result.qualifies = len(result.reasons) > 0
    return result


async def _fetch_page(url: str, timeout: float) -> tuple[str | None, float, int | None]:
    """Fetch a webpage using Playwright for accurate JS-rendered HTML.
    Falls back to httpx if Playwright fails.
    """
    html, load_time, status = await _fetch_page_browser(url, timeout)
    if html and len(html) > 500:
        return html, load_time, status

    start = time.monotonic()
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            verify=False,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            response = await client.get(url)
            load_time = time.monotonic() - start
            return response.text, load_time, response.status_code
    except Exception as e:
        logger.debug(f"httpx fallback also failed for {url}: {e}")
        return html, load_time, status


async def _fetch_page_browser(url: str, timeout: float) -> tuple[str | None, float, int | None]:
    """Fetch a webpage using a real browser to get fully rendered HTML."""
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth
    from src.utils import get_random_user_agent

    start = time.monotonic()
    stealth = Stealth()
    try:
        async with stealth.use_async(async_playwright()) as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(
                user_agent=get_random_user_agent(),
                viewport={"width": 1366, "height": 768},
            )
            page = await ctx.new_page()
            response = await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout * 1000))
            await page.wait_for_timeout(2000)
            html = await page.content()
            load_time = time.monotonic() - start
            status = response.status if response else 200
            await browser.close()
            return html, load_time, status
    except Exception as e:
        load_time = time.monotonic() - start
        logger.debug(f"Browser fetch failed for {url}: {e}")
        return None, load_time, None


def _is_chain_or_franchise(html_lower: str, soup: BeautifulSoup) -> bool:
    for pattern in CHAIN_INDICATORS:
        if re.search(pattern, html_lower, re.I):
            return True

    location_links = soup.find_all("a", href=re.compile(r"location|store-finder|branches", re.I))
    if len(location_links) > 3:
        return True

    return False


def _is_ecommerce(html_lower: str, soup: BeautifulSoup) -> bool:
    score = 0
    for pattern in ECOMMERCE_INDICATORS:
        if re.search(pattern, html_lower, re.I):
            score += 1
    return score >= 2


def _has_outdated_patterns(html_lower: str) -> bool:
    score = 0
    for pattern in OUTDATED_PATTERNS:
        if re.search(pattern, html_lower, re.I):
            score += 1
    return score >= 3


def _get_copyright_year(html: str) -> int | None:
    matches = COPYRIGHT_YEAR_PATTERN.findall(html)
    if matches:
        try:
            return int(matches[-1])
        except ValueError:
            pass
    return None


def _find_contact_email(emails: list[str]) -> str | None:
    """Pick the most likely contact email from a list of found emails."""
    skip_patterns = [
        "noreply", "no-reply", "info@example", "user@example",
        "email@example", "support@wordpress", "wix.com",
    ]
    for email in emails:
        if any(skip in email.lower() for skip in skip_patterns):
            continue
        if len(email) > 6:
            return email
    return None


def _has_broken_layout(soup: BeautifulSoup, html_lower: str) -> bool:
    """Detect broken layout indicators, accounting for modern web practices."""
    imgs = soup.find_all("img")
    truly_broken = 0
    for img in imgs[:20]:
        src = img.get("src", "")
        data_src = img.get("data-src", "") or img.get("data-lazy", "") or img.get("loading", "")
        # Only count as broken if no src AND no lazy-loading attribute
        if (not src or (src.startswith("data:") and len(src) < 50)) and not data_src:
            truly_broken += 1
    if truly_broken >= 5:
        return True

    stylesheets = soup.find_all("link", rel="stylesheet")
    inline_styles = html_lower.count("<style")
    has_css_modules = "css-" in html_lower or "_css" in html_lower
    has_styled = "sc-" in html_lower or "styled-" in html_lower
    if len(stylesheets) == 0 and inline_styles == 0 and not has_css_modules and not has_styled:
        if len(html_lower) > 2000:
            return True

    return False


async def qualify_businesses(
    businesses: list[Business],
    max_concurrent: int = 5,
    use_ai: bool = False,
    **kwargs,
) -> list[tuple[Business, QualificationResult]]:
    """Qualify a batch of businesses with concurrency control.

    If use_ai=True, websites that pass rule-based checks (would be SKIPPED)
    get a second-pass AI vision analysis to catch outdated designs that
    rules alone can't detect.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    results = []

    async def _qualify_one(biz: Business) -> tuple[Business, QualificationResult]:
        async with semaphore:
            result = await qualify_business(biz, **kwargs)
            status = "QUALIFIED" if result.qualifies else "SKIPPED"
            reason = ", ".join(result.reasons) if result.qualifies else (result.disqualify_reason or "Good website")
            logger.info(f"  [{status}] {biz.name}: {reason}")
            return biz, result

    tasks = [_qualify_one(biz) for biz in businesses]
    completed = await asyncio.gather(*tasks, return_exceptions=True)

    for item in completed:
        if isinstance(item, Exception):
            logger.error(f"Qualification error: {item}")
            continue
        results.append(item)

    qualified_count = sum(1 for _, r in results if r.qualifies)
    logger.info(f"Rule-based qualification: {qualified_count}/{len(results)} qualified")

    if not use_ai:
        return results

    # AI second pass: score websites that rules said "Good website" (SKIPPED)
    # These are businesses with websites that didn't trigger any rule -- AI checks
    # if the design actually looks outdated visually
    needs_ai_check = []
    for biz, result in results:
        if (
            not result.qualifies
            and not result.disqualify_reason
            and biz.website
            and result.has_website
        ):
            needs_ai_check.append((biz, result))

    if not needs_ai_check:
        logger.info("No websites need AI scoring")
        return results

    logger.info(f"\nRunning AI vision analysis on {len(needs_ai_check)} websites...")

    try:
        from src.ai_scorer import AIWebsiteScorer
        scorer = AIWebsiteScorer()

        websites = [
            {
                "url": biz.website,
                "business_name": biz.name,
                "niche": biz.niche,
                "city": biz.city,
            }
            for biz, _ in needs_ai_check
        ]

        ai_results = await scorer.score_batch(websites)

        # Map AI results back by URL
        ai_map = {}
        for site_dict, ai_result in ai_results:
            ai_map[site_dict["url"]] = ai_result

        # Update qualification results with AI scores
        updated_results = []
        for biz, result in results:
            if biz.website and biz.website in ai_map:
                ai = ai_map[biz.website]
                result.ai_score = ai.overall_score
                result.ai_design_score = ai.design_score
                result.ai_mobile_score = ai.mobile_score
                result.ai_professionalism_score = ai.professionalism_score
                result.ai_cta_score = ai.cta_score
                result.ai_summary = ai.summary
                result.ai_issues = ai.issues

                if ai.qualifies and not ai.error:
                    result.qualifies = True
                    ai_reasons = []
                    if ai.overall_score <= 4:
                        ai_reasons.append(f"AI: Poor design ({ai.overall_score}/10)")
                    elif ai.overall_score <= 5:
                        ai_reasons.append(f"AI: Mediocre design ({ai.overall_score}/10)")
                    if ai.issues:
                        ai_reasons.extend(f"AI: {issue}" for issue in ai.issues[:2])
                    result.reasons.extend(ai_reasons)
                    logger.info(
                        f"  [AI QUALIFIED] {biz.name}: "
                        f"score={ai.overall_score}/10 - {ai.summary}"
                    )
                else:
                    logger.info(
                        f"  [AI CONFIRMED GOOD] {biz.name}: "
                        f"score={ai.overall_score}/10 - {ai.summary}"
                    )

            updated_results.append((biz, result))

        ai_qualified = sum(
            1 for biz, r in updated_results
            if r.qualifies and r.ai_score is not None
        )
        total_qualified = sum(1 for _, r in updated_results if r.qualifies)
        logger.info(
            f"After AI scoring: {total_qualified} total qualified "
            f"({ai_qualified} caught by AI alone)"
        )
        return updated_results

    except Exception as e:
        logger.error(f"AI scoring failed, falling back to rules only: {e}")
        return results
