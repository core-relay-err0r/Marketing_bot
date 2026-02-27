import asyncio
import click
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from src.utils import setup_logging

load_dotenv()
console = Console()


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def cli(verbose: bool):
    """Burra.io Lead Generation Bot

    Scrape Google Maps, qualify leads, add to tracker, send outreach emails.
    """
    setup_logging(verbose)


@cli.command()
@click.option("--city", "-c", default=None, help="Target city (e.g., Miami)")
@click.option("--country", default=None, help="Target country (e.g., USA)")
@click.option("--niche", "-n", default=None, help="Target niche (e.g., dentists)")
@click.option("--no-email", is_flag=True, help="Skip email sending")
@click.option("--no-headless", is_flag=True, help="Show browser window (for debugging)")
@click.option("--ai", is_flag=True, help="Use AI vision model to score website design quality")
def run(city, country, niche, no_email, no_headless, ai):
    """Run the full lead generation pipeline.

    Without arguments, uses daily city rotation and top 3 priority niches.
    Use --ai to enable AI-powered website design scoring via Vercel AI Gateway.
    """
    from src.orchestrator import run_pipeline

    console.print(Panel.fit(
        "[bold green]Web Scraper[/bold green]\n"
        "Starting full pipeline..." +
        ("\n[bold magenta]AI Vision Scoring: ON[/bold magenta]" if ai else ""),
        border_style="green",
    ))

    if city:
        console.print(f"  Target: [cyan]{niche or 'top 3 niches'}[/cyan] in [cyan]{city}[/cyan], [cyan]{country or 'USA'}[/cyan]")
    else:
        console.print("  Using daily rotation schedule")

    stats = asyncio.run(run_pipeline(
        city=city,
        country=country,
        niche=niche,
        headless=not no_headless,
        send_emails=not no_email,
        use_ai=ai,
    ))

    _print_stats(stats)


@cli.command()
@click.option("--sheet-tab", default=None, help="Worksheet tab name (default: today's date)")
def email(sheet_tab):
    """Send outreach emails to un-emailed leads in the tracker."""
    from src.orchestrator import run_email_only

    console.print(Panel.fit(
        "[bold blue]Email Outreach Mode[/bold blue]\n"
        f"Sending to unsent leads in '{sheet_tab or 'today'}'",
        border_style="blue",
    ))

    result = asyncio.run(run_email_only(worksheet_title=sheet_tab))
    console.print(f"\n[green]Emails sent: {result['emails_sent']}[/green]")


@cli.command()
def status():
    """Show current pipeline status and daily progress."""
    from src.orchestrator import get_status

    info = get_status()

    table = Table(title="Daily Status", border_style="cyan")
    table.add_column("Metric", style="bold")
    table.add_column("Value", style="green")

    table.add_row("Date", info.get("date", "?"))
    table.add_row("Worksheet", info.get("worksheet", "?"))

    cities = info.get("rotation_cities", [])
    city_str = ", ".join(f"{c['city']} ({c['country']})" for c in cities)
    table.add_row("Today's Cities", city_str or "N/A")

    niches = info.get("priority_niches", [])
    table.add_row("Priority Niches", ", ".join(niches) or "N/A")

    table.add_row("Leads in Sheet", str(info.get("total", "?")))
    table.add_row("Emails Sent", str(info.get("emailed", "?")))
    table.add_row("Pending Email", str(info.get("pending_email", "?")))

    if "sheet_error" in info:
        table.add_row("Sheet Error", f"[red]{info['sheet_error']}[/red]")

    console.print(table)


@cli.command()
@click.argument("csv_file", type=click.Path(exists=True))
@click.option("--niche", "-n", required=True, help="Niche for qualification context")
@click.option("--city", "-c", required=True, help="City for these leads")
@click.option("--country", default="USA", help="Country for these leads")
@click.option("--no-email", is_flag=True, help="Skip email sending")
def qualify(csv_file, niche, city, country, no_email):
    """Qualify leads from a CSV file (from Instant Data Scraper export)."""
    import csv
    from src.models import Business
    from src.qualifier import qualify_businesses
    from src.dedup import DeduplicationEngine
    from src.sheets import SheetsManager
    from src.orchestrator import _build_lead, _worksheet_title_for_today

    console.print(Panel.fit(
        "[bold yellow]CSV Qualification Mode[/bold yellow]\n"
        f"Processing: {csv_file}",
        border_style="yellow",
    ))

    businesses = []
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (
                row.get("Business Name") or row.get("name") or
                row.get("Title") or row.get("title") or ""
            ).strip()
            if not name:
                continue

            biz = Business(
                name=name,
                address=row.get("Address", row.get("address", "")),
                city=city,
                country=country,
                phone=row.get("Phone", row.get("phone", "")),
                website=row.get("Website", row.get("website", row.get("url", ""))),
                niche=niche,
            )
            businesses.append(biz)

    console.print(f"Loaded [cyan]{len(businesses)}[/cyan] businesses from CSV")

    async def _run():
        qualified_pairs = await qualify_businesses(businesses)
        qualified = [(b, r) for b, r in qualified_pairs if r.qualifies]

        console.print(f"Qualified: [green]{len(qualified)}[/green]/{len(businesses)}")

        sheets = SheetsManager()
        dedup = DeduplicationEngine()
        try:
            existing = sheets.get_all_leads_all_sheets()
            dedup.load_existing_leads(existing)
        except Exception:
            pass

        unique = [(b, r) for b, r in qualified if not dedup.is_duplicate(b)]
        leads = [_build_lead(b, r) for b, r in unique]

        ws_title = _worksheet_title_for_today()
        try:
            added = sheets.append_leads(ws_title, leads)
            console.print(f"Added [green]{added}[/green] leads to sheet '{ws_title}'")
        except Exception as e:
            console.print(f"[red]Sheet error: {e}[/red]")

        return {"qualified": len(qualified), "unique": len(unique), "added": len(leads)}

    asyncio.run(_run())


@cli.command("test-email")
@click.option("--to", "to_email", required=True, help="Your test email address")
@click.option("--sheet-tab", default=None, help="Worksheet tab to pull leads from (default: today)")
@click.option("--count", "-n", default=5, help="Number of test emails to send (default: 5)")
def test_email(to_email, sheet_tab, count):
    """Send test emails using real leads from the sheet, redirected to your inbox.

    Picks leads with diverse weakness types so you can review the personalization.
    """
    from src.sheets import SheetsManager
    from src.emailer import EmailSender
    from src.orchestrator import _worksheet_title_for_today

    ws_title = sheet_tab or _worksheet_title_for_today()

    console.print(Panel.fit(
        "[bold yellow]Email Test Mode[/bold yellow]\n"
        f"Sheet: '{ws_title}' → All emails go to [cyan]{to_email}[/cyan]",
        border_style="yellow",
    ))

    sheets = SheetsManager()
    try:
        records = sheets.get_all_leads(ws_title)
    except Exception as e:
        console.print(f"[red]Could not read sheet: {e}[/red]")
        return

    if not records:
        console.print("[red]No leads found in this worksheet.[/red]")
        return

    console.print(f"Found [cyan]{len(records)}[/cyan] leads in '{ws_title}'")

    # Pick diverse leads with different weakness types
    seen_types = set()
    selected = []
    priority_keywords = ["no website", "social media", "outdated", "mobile", "slow",
                         "broken", "poor design", "free email", "ai:"]

    for record in records:
        reasons = str(record.get("Qualification Reasons", "")).lower()
        for kw in priority_keywords:
            if kw in reasons and kw not in seen_types:
                seen_types.add(kw)
                selected.append(record)
                break
        if len(selected) >= count:
            break

    # Fill remaining slots if we haven't hit count
    if len(selected) < count:
        for record in records:
            if record not in selected:
                selected.append(record)
            if len(selected) >= count:
                break

    console.print(f"Selected [cyan]{len(selected)}[/cyan] leads with diverse weaknesses:\n")
    for i, r in enumerate(selected, 1):
        reasons = r.get("Qualification Reasons", "N/A")
        console.print(f"  {i}. [bold]{r.get('Business Name', '?')}[/bold]")
        console.print(f"     Weakness: [yellow]{reasons}[/yellow]")
        console.print(f"     Website: {r.get('Website', 'None')}")
        console.print()

    async def _send_tests():
        emailer = EmailSender()
        sent = 0
        for i, record in enumerate(selected, 1):
            biz_name = record.get("Business Name", "Unknown")
            console.print(f"[{i}/{len(selected)}] Generating personalized email for [bold]{biz_name}[/bold]...")

            ai_score_raw = record.get("AI Score", "")
            ai_score_int = None
            if ai_score_raw:
                try:
                    ai_score_int = int(str(ai_score_raw).replace("/10", "").strip())
                except ValueError:
                    pass

            result = await emailer.send_email(
                to_email=to_email,
                business_name=biz_name,
                niche=record.get("Niche", ""),
                website=record.get("Website", ""),
                city=record.get("City", ""),
                qualification_reasons=record.get("Qualification Reasons", ""),
                ai_score=ai_score_int,
                ai_summary=record.get("AI Summary", ""),
            )

            if "error" not in result:
                sent += 1
                console.print(f"  [green]Sent![/green]")
            else:
                console.print(f"  [red]Failed: {result['error']}[/red]")

            if i < len(selected):
                await asyncio.sleep(2)

        return sent

    sent = asyncio.run(_send_tests())
    console.print(f"\n[bold green]Done! {sent}/{len(selected)} test emails sent to {to_email}[/bold green]")
    console.print("[dim]Check your inbox (and spam folder) to review the personalized emails.[/dim]")


def _print_stats(stats: dict):
    table = Table(title="Pipeline Results", border_style="green")
    table.add_column("Metric", style="bold")
    table.add_column("Count", justify="right", style="cyan")

    table.add_row("Raw Scraped", str(stats.get("scraped", 0)))
    table.add_row("Qualified", str(stats.get("qualified", 0)))
    table.add_row("Duplicates Removed", str(stats.get("duplicates_removed", 0)))
    table.add_row("Added to Sheet", str(stats.get("added_to_sheet", 0)))
    table.add_row("Emails Sent", str(stats.get("emails_sent", 0)))

    console.print(table)

    errors = stats.get("errors", [])
    if errors:
        console.print(f"\n[red]Errors ({len(errors)}):[/red]")
        for err in errors:
            console.print(f"  [red]- {err}[/red]")


if __name__ == "__main__":
    cli()
