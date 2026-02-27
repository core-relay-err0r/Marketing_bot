import asyncio
import logging
import re
from urllib.parse import quote_plus

from playwright.async_api import async_playwright, Page, BrowserContext
from playwright_stealth import Stealth

from src.models import Business
from src.utils import get_random_user_agent, random_delay

logger = logging.getLogger("leadgen.scraper")

GOOGLE_MAPS_URL = "https://www.google.com/maps/search/"


async def _create_stealth_context(playwright, headless: bool = True) -> BrowserContext:
    browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-sync",
            "--disable-translate",
            "--no-first-run",
            "--disable-software-rasterizer",
            "--disable-setuid-sandbox",
        ],
    )
    context = await browser.new_context(
        user_agent=get_random_user_agent(),
        viewport={"width": 1366, "height": 768},
        locale="en-US",
        timezone_id="America/New_York",
    )
    return context


async def _accept_cookies(page: Page) -> None:
    """Dismiss Google's cookie consent if it appears."""
    try:
        accept_btn = page.locator("button", has_text=re.compile(r"accept|agree|consent", re.I))
        if await accept_btn.count() > 0:
            await accept_btn.first.click()
            await random_delay(1, 2)
    except Exception:
        pass


async def _scroll_results(page: Page, max_results: int = 120) -> int:
    """Scroll the Maps results panel to load more businesses."""
    results_selector = 'div[role="feed"]'
    try:
        feed = page.locator(results_selector)
        if await feed.count() == 0:
            feed = page.locator('div[role="main"]')
    except Exception:
        return 0

    previous_count = 0
    stale_rounds = 0

    for _ in range(60):
        items = page.locator('a[href*="/maps/place/"]')
        current_count = await items.count()

        if current_count >= max_results:
            logger.info(f"Reached {current_count} results, stopping scroll")
            break

        if current_count == previous_count:
            stale_rounds += 1
            if stale_rounds >= 4:
                end_marker = page.locator("p.fontBodyMedium span", has_text=re.compile(r"end of|no more", re.I))
                if await end_marker.count() > 0:
                    logger.info("Reached end of results list")
                    break
                # Also check for the "You've reached the end" text
                end_marker2 = page.locator("span", has_text="You've reached the end of the list")
                if await end_marker2.count() > 0:
                    logger.info("Reached end of results list")
                    break
                if stale_rounds >= 6:
                    logger.info("No new results after multiple scroll attempts")
                    break
        else:
            stale_rounds = 0

        previous_count = current_count

        try:
            await feed.evaluate("el => el.scrollTop = el.scrollHeight")
        except Exception:
            await page.keyboard.press("End")

        await random_delay(1.5, 3.0)

    final_count = await page.locator('a[href*="/maps/place/"]').count()
    logger.info(f"Total results after scrolling: {final_count}")
    return final_count


async def _extract_businesses(page: Page, niche: str, city: str, country: str) -> list[Business]:
    """Extract business data from the currently loaded Maps results."""
    businesses = []

    items = page.locator('a[href*="/maps/place/"]')
    count = await items.count()
    logger.info(f"Extracting data from {count} business listings")

    for i in range(count):
        try:
            item = items.nth(i)
            aria_label = await item.get_attribute("aria-label") or ""

            name = aria_label.strip()
            if not name:
                continue

            try:
                await item.scroll_into_view_if_needed(timeout=10000)
            except Exception:
                pass
            await random_delay(0.3, 0.5)

            try:
                await item.click(timeout=15000)
            except Exception:
                logger.warning(f"  [{i+1}/{count}] Click failed, skipping: {name}")
                continue
            await random_delay(1.5, 2.5)

            detail_loaded = False
            for selector in [
                '[data-item-id="authority"]',
                '[data-item-id="address"]',
                'button[data-item-id*="phone"]',
                '[data-item-id]',
            ]:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    detail_loaded = True
                    break
                except Exception:
                    continue

            if not detail_loaded:
                await random_delay(2.0, 3.0)

            phone = await _extract_phone(page)
            website = await _extract_website(page)
            address = await _extract_address(page)
            rating = await _extract_rating(page)
            review_count = await _extract_review_count(page)
            category = await _extract_category(page)

            logger.info(
                f"  [{i+1}/{count}] {name} | "
                f"phone={'Y' if phone else 'N'} | "
                f"web={'Y' if website else 'N'} | "
                f"addr={'Y' if address else 'N'}"
            )

            biz = Business(
                name=name,
                address=address or "",
                city=city,
                country=country,
                phone=phone,
                website=website,
                rating=rating,
                review_count=review_count,
                category=category,
                niche=niche,
            )
            businesses.append(biz)

        except Exception as e:
            logger.warning(f"  [{i+1}/{count}] Failed: {e}")
            continue

    return businesses


async def _extract_phone(page: Page) -> str | None:
    """Extract phone number from the detail panel."""
    # Method 1: data-item-id containing "phone" (e.g., "phone:tel:+18172676542")
    try:
        el = page.locator('[data-item-id*="phone"]')
        if await el.count() > 0:
            # Try to get phone from data-item-id itself (format: "phone:tel:+1234567890")
            item_id = await el.first.get_attribute("data-item-id") or ""
            tel_match = re.search(r'tel:(\+?[\d]+)', item_id)
            if tel_match:
                raw = tel_match.group(1)
                # Format nicely: +1 XXX-XXX-XXXX
                if len(raw) >= 10:
                    return raw

            # Fallback: get from aria-label or text content
            text = await el.first.get_attribute("aria-label") or await el.first.inner_text()
            match = re.search(r'(\+?[\d\s\-\(\)\.]{7,})', text)
            if match:
                return match.group(1).strip()
    except Exception:
        pass

    # Method 2: scan all elements with data-item-id
    try:
        info_items = page.locator('[data-item-id]')
        for j in range(await info_items.count()):
            item_id = await info_items.nth(j).get_attribute("data-item-id") or ""
            if "phone" in item_id.lower():
                tel_match = re.search(r'tel:(\+?[\d]+)', item_id)
                if tel_match:
                    return tel_match.group(1)
                text = await info_items.nth(j).get_attribute("aria-label") or ""
                match = re.search(r'(\+?[\d\s\-\(\)\.]{7,})', text)
                if match:
                    return match.group(1).strip()
    except Exception:
        pass

    return None


async def _extract_website(page: Page) -> str | None:
    """Extract website URL from the detail panel."""
    # Method 1: website link with data-item-id
    selectors = [
        'a[data-item-id="authority"]',
        'a[data-tooltip="Open website"]',
        'a[aria-label*="Website" i]',
        'a[data-item-id*="website"]',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel)
            if await el.count() > 0:
                href = await el.first.get_attribute("href")
                if href and href.startswith("http"):
                    # Skip Google redirect links, extract actual URL
                    if "google.com/url" in href:
                        match = re.search(r'[?&]q=([^&]+)', href)
                        if match:
                            from urllib.parse import unquote
                            return unquote(match.group(1))
                    return href
                # Sometimes the URL is in aria-label or text
                text = await el.first.get_attribute("aria-label") or await el.first.inner_text()
                text = text.strip()
                if text and "." in text and " " not in text:
                    if not text.startswith("http"):
                        text = "https://" + text
                    return text
        except Exception:
            continue

    # Method 2: scan info items for authority/website
    try:
        info_items = page.locator('div[role="main"] a[data-item-id]')
        for j in range(await info_items.count()):
            item_id = await info_items.nth(j).get_attribute("data-item-id") or ""
            if item_id in ("authority", "website"):
                href = await info_items.nth(j).get_attribute("href")
                if href and href.startswith("http"):
                    return href
                text = await info_items.nth(j).inner_text()
                if text and "." in text:
                    return "https://" + text.strip() if not text.startswith("http") else text.strip()
    except Exception:
        pass

    return None


async def _extract_address(page: Page) -> str | None:
    """Extract address from the detail panel."""
    selectors = [
        'button[data-item-id="address"]',
        'button[data-tooltip="Copy address"]',
        'button[data-item-id*="address"]',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel)
            if await el.count() > 0:
                text = await el.first.get_attribute("aria-label") or await el.first.inner_text()
                # Strip common prefixes in any language (Address:, ที่อยู่:, etc.)
                cleaned = re.sub(r'^[^:]+:\s*', '', text).strip()
                if not cleaned or len(cleaned) < 5:
                    cleaned = text.strip()
                # Remove non-Latin trailing text (e.g. Thai country name)
                cleaned = re.sub(r'[^\x00-\x7F]+$', '', cleaned).strip().rstrip(',')
                return cleaned
        except Exception:
            continue
    return None


async def _extract_rating(page: Page) -> float | None:
    """Extract star rating from the detail panel."""
    # Method 1: large display number
    try:
        el = page.locator('div.fontDisplayLarge')
        if await el.count() > 0:
            text = (await el.first.inner_text()).strip()
            val = float(text.replace(",", "."))
            if 0 < val <= 5:
                return val
    except Exception:
        pass

    # Method 2: aria-label with stars
    try:
        el = page.locator('span[role="img"][aria-label*="star"]')
        if await el.count() > 0:
            label = await el.first.get_attribute("aria-label") or ""
            match = re.search(r"([\d.,]+)\s*star", label, re.I)
            if match:
                return float(match.group(1).replace(",", "."))
    except Exception:
        pass

    return None


async def _extract_review_count(page: Page) -> int | None:
    """Extract review count from the detail panel."""
    try:
        el = page.locator('button[jsaction*="review"]')
        if await el.count() > 0:
            text = await el.first.inner_text()
            match = re.search(r"([\d,]+)\s*review", text, re.I)
            if match:
                return int(match.group(1).replace(",", ""))
            # Sometimes just a number in parentheses
            match = re.search(r'\(([\d,]+)\)', text)
            if match:
                return int(match.group(1).replace(",", ""))
    except Exception:
        pass

    try:
        el = page.locator('span[aria-label*="review"]')
        if await el.count() > 0:
            label = await el.first.get_attribute("aria-label") or ""
            match = re.search(r"([\d,]+)\s*review", label, re.I)
            if match:
                return int(match.group(1).replace(",", ""))
    except Exception:
        pass

    return None


async def _extract_category(page: Page) -> str | None:
    """Extract business category from the detail panel."""
    selectors = [
        'button[jsaction*="category"]',
        'span.fontBodyMedium button[jsaction*="pane"]',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel)
            if await el.count() > 0:
                text = (await el.first.inner_text()).strip()
                if text and len(text) < 60:
                    return text
        except Exception:
            continue
    return None


async def scrape_google_maps(
    niche: str,
    city: str,
    country: str,
    max_results: int = 120,
    headless: bool = True,
) -> list[Business]:
    """Scrape Google Maps for businesses matching niche + city."""
    query = f"{niche} in {city}"
    encoded = quote_plus(query)
    url = f"{GOOGLE_MAPS_URL}{encoded}/"

    logger.info(f"Scraping Google Maps: '{query}' (max {max_results} results)")

    try:
        return await asyncio.wait_for(
            _scrape_google_maps_inner(niche, city, country, max_results, headless, url, query),
            timeout=600,
        )
    except asyncio.TimeoutError:
        logger.error(f"Scraping timed out after 10 minutes for '{query}'")
        return []


async def _scrape_google_maps_inner(
    niche: str, city: str, country: str,
    max_results: int, headless: bool, url: str, query: str,
) -> list[Business]:
    stealth = Stealth()
    async with stealth.use_async(async_playwright()) as pw:
        context = await _create_stealth_context(pw, headless=headless)
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await random_delay(2, 4)
            await _accept_cookies(page)
            await random_delay(1, 2)

            await _scroll_results(page, max_results)
            businesses = await _extract_businesses(page, niche, city, country)

            logger.info(f"Scraped {len(businesses)} businesses for '{query}'")
            return businesses

        except Exception as e:
            logger.error(f"Scraping failed for '{query}': {e}")
            return []

        finally:
            await context.browser.close()


async def scrape_multiple(
    targets: list[dict],
    max_results: int = 120,
    headless: bool = True,
) -> list[Business]:
    """Scrape multiple niche+city combinations sequentially."""
    all_businesses = []
    for target in targets:
        businesses = await scrape_google_maps(
            niche=target["niche"],
            city=target["city"],
            country=target["country"],
            max_results=max_results,
            headless=headless,
        )
        all_businesses.extend(businesses)
        if len(targets) > 1:
            await random_delay(5, 10)
    return all_businesses
