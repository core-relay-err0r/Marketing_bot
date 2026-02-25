import asyncio
import base64
import json
import logging
import os
import re
from dataclasses import dataclass

from openai import AsyncOpenAI
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from src.utils import get_random_user_agent

logger = logging.getLogger("leadgen.ai_scorer")

SYSTEM_PROMPT = """You are a website quality assessor for a web design agency that targets small-medium local businesses. 
Your job is to evaluate a website screenshot and determine if the business would benefit from a new website.

Evaluate the website on these criteria:
1. **Design Quality** (1-10): Is the design modern, clean, and professional? Or does it look dated/amateur?
2. **Mobile Readiness** (1-10): Does it appear responsive and mobile-friendly?
3. **Professionalism** (1-10): Does it look trustworthy? Good typography, imagery, layout?
4. **Call-to-Action** (1-10): Are there clear CTAs (book now, call us, contact form)?
5. **Overall Score** (1-10): Overall website quality.

A score of 1-4 means the website is poor/outdated and the business NEEDS a new website.
A score of 5-6 means the website is mediocre and COULD benefit from a redesign.
A score of 7-10 means the website is good/modern and does NOT need our services.

Respond ONLY with valid JSON in this exact format:
{
  "design_score": <1-10>,
  "mobile_score": <1-10>,
  "professionalism_score": <1-10>,
  "cta_score": <1-10>,
  "overall_score": <1-10>,
  "needs_new_website": <true/false>,
  "issues": ["issue 1", "issue 2"],
  "summary": "One sentence summary of website quality"
}"""

USER_PROMPT = """Evaluate this website screenshot for a {niche} business called "{business_name}" in {city}.
Is this a website that looks like it needs a redesign? Score it honestly."""


@dataclass
class AIScoreResult:
    design_score: int = 5
    mobile_score: int = 5
    professionalism_score: int = 5
    cta_score: int = 5
    overall_score: int = 5
    needs_new_website: bool = False
    issues: list[str] = None
    summary: str = ""
    error: str | None = None

    def __post_init__(self):
        if self.issues is None:
            self.issues = []

    @property
    def qualifies(self) -> bool:
        return self.needs_new_website or self.overall_score <= 5


class AIWebsiteScorer:
    def __init__(
        self,
        api_key: str | None = None,
        model: str = "anthropic/claude-sonnet-4.6",
        max_concurrent: int = 3,
    ):
        self.api_key = api_key or os.getenv("VERCEL_AI_GATEWAY_KEY", "")
        self.model = model
        self.max_concurrent = max_concurrent
        self._client: AsyncOpenAI | None = None

    def _get_client(self) -> AsyncOpenAI:
        if self._client is None:
            if not self.api_key:
                raise ValueError(
                    "Vercel AI Gateway API key not set. "
                    "Set VERCEL_AI_GATEWAY_KEY in your .env file."
                )
            self._client = AsyncOpenAI(
                api_key=self.api_key,
                base_url="https://ai-gateway.vercel.sh/v1",
            )
        return self._client

    async def _take_screenshot(self, url: str, timeout: int = 15000) -> bytes | None:
        """Take a screenshot of a website using Playwright."""
        if not url.startswith("http"):
            url = "https://" + url

        stealth = Stealth()
        async with stealth.use_async(async_playwright()) as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                # Desktop screenshot
                ctx = await browser.new_context(
                    user_agent=get_random_user_agent(),
                    viewport={"width": 1366, "height": 900},
                )
                page = await ctx.new_page()
                await page.goto(url, wait_until="networkidle", timeout=timeout)
                await asyncio.sleep(1)
                screenshot = await page.screenshot(full_page=False, type="jpeg", quality=70)
                return screenshot
            except Exception as e:
                logger.debug(f"Screenshot failed for {url}: {e}")
                return None
            finally:
                await browser.close()

    async def score_website(
        self,
        url: str,
        business_name: str = "",
        niche: str = "",
        city: str = "",
    ) -> AIScoreResult:
        """Score a website using AI vision analysis."""
        screenshot = await self._take_screenshot(url)
        if screenshot is None:
            return AIScoreResult(
                error="Could not take screenshot",
                needs_new_website=True,
                overall_score=1,
                summary="Website could not be loaded for screenshot",
                issues=["Website unreachable or broken"],
            )

        b64_image = base64.b64encode(screenshot).decode("utf-8")
        client = self._get_client()

        try:
            response = await client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": USER_PROMPT.format(
                                    niche=niche or "local",
                                    business_name=business_name or "Unknown",
                                    city=city or "Unknown",
                                ),
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{b64_image}",
                                },
                            },
                        ],
                    },
                ],
                max_tokens=500,
                temperature=0.1,
            )

            content = response.choices[0].message.content.strip()
            # Extract JSON from response (handle markdown code blocks)
            json_match = re.search(r'\{[\s\S]*\}', content)
            if json_match:
                data = json.loads(json_match.group())
            else:
                raise ValueError(f"No JSON found in response: {content[:200]}")

            result = AIScoreResult(
                design_score=int(data.get("design_score", 5)),
                mobile_score=int(data.get("mobile_score", 5)),
                professionalism_score=int(data.get("professionalism_score", 5)),
                cta_score=int(data.get("cta_score", 5)),
                overall_score=int(data.get("overall_score", 5)),
                needs_new_website=bool(data.get("needs_new_website", False)),
                issues=data.get("issues", []),
                summary=data.get("summary", ""),
            )

            logger.info(
                f"  [AI] {business_name}: score={result.overall_score}/10 "
                f"needs_redesign={'YES' if result.needs_new_website else 'NO'} "
                f"- {result.summary}"
            )
            return result

        except Exception as e:
            logger.error(f"AI scoring failed for {url}: {e}")
            return AIScoreResult(error=str(e))

    async def score_batch(
        self,
        websites: list[dict],
    ) -> list[tuple[dict, AIScoreResult]]:
        """Score multiple websites with concurrency control.

        Each dict should have: url, business_name, niche, city
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        results = []

        async def _score_one(site: dict) -> tuple[dict, AIScoreResult]:
            async with semaphore:
                result = await self.score_website(
                    url=site["url"],
                    business_name=site.get("business_name", ""),
                    niche=site.get("niche", ""),
                    city=site.get("city", ""),
                )
                return site, result

        tasks = [_score_one(site) for site in websites]
        completed = await asyncio.gather(*tasks, return_exceptions=True)

        for item in completed:
            if isinstance(item, Exception):
                logger.error(f"Batch scoring error: {item}")
                continue
            results.append(item)

        needs_redesign = sum(1 for _, r in results if r.qualifies)
        logger.info(
            f"AI scoring complete: {needs_redesign}/{len(results)} need redesign"
        )
        return results
