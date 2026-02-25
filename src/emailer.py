import asyncio
import json
import logging
import os
import random
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path

from openai import AsyncOpenAI

logger = logging.getLogger("leadgen.emailer")

PERSONALIZATION_PROMPT = """You are writing a short email paragraph for Burra.io, a web design agency.

VERIFIED FACTS about this business (from our automated checks — these are the ONLY things you know for certain):
{fact_sheet}

Business context:
- Name: {business_name}
- Industry: {niche}
- Location: {city}

STRICT RULES — you MUST follow all of these:
1. You may ONLY reference weaknesses listed as TRUE in the VERIFIED FACTS above
2. NEVER claim something that is not verified — if "has_website" is TRUE, NEVER say they don't have a website
3. NEVER invent or assume problems not listed (e.g., don't say "your site looks datied" unless outdated_design is TRUE)
4. If the only issue is "uses_free_email", do NOT criticize their website design — it may be perfectly fine
5. Write 3-5 sentences of flowing prose (no bullet points, no numbered lists)
6. Do NOT start with "I noticed" (the sentence before this paragraph already covers that)
7. Do NOT mention the $600 price or 48-hour timeline (that appears later in the email)
8. Keep it under 80 words
9. Be warm and helpful, not salesy — like friendly advice from someone in the industry
10. Explain the business impact of the VERIFIED weakness (losing customers, credibility, etc.)
11. Do NOT wrap your response in quotation marks

Respond with ONLY the paragraph text."""


class EmailSender:
    def __init__(
        self,
        sender_email: str | None = None,
        sender_name: str | None = None,
        smtp_host: str | None = None,
        smtp_port: int | None = None,
        smtp_password: str | None = None,
        template_path: str = "templates/outreach_email.txt",
        daily_limit: int = 80,
        delay_min: int = 30,
        delay_max: int = 60,
        ai_api_key: str | None = None,
        ai_model: str = "anthropic/claude-sonnet-4.6",
    ):
        self.sender_email = sender_email or os.getenv("SENDER_EMAIL", "")
        self.sender_name = sender_name or os.getenv("SENDER_NAME", "")
        self.smtp_host = smtp_host or os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = smtp_port or int(os.getenv("SMTP_PORT", "587"))
        self.smtp_password = smtp_password or os.getenv("SMTP_PASSWORD", "")
        self.template_path = template_path
        self.daily_limit = daily_limit
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.ai_api_key = ai_api_key or os.getenv("VERCEL_AI_GATEWAY_KEY", "")
        self.ai_model = ai_model
        self._sent_today = 0
        self._template: str | None = None
        self._ai_client: AsyncOpenAI | None = None

    def _get_ai_client(self) -> AsyncOpenAI | None:
        if not self.ai_api_key:
            return None
        if self._ai_client is None:
            self._ai_client = AsyncOpenAI(
                api_key=self.ai_api_key,
                base_url="https://ai-gateway.vercel.sh/v1",
            )
        return self._ai_client

    def _load_template(self) -> tuple[str, str, str | None]:
        """Load email templates. Returns (subject, plain_body, html_body)."""
        if self._template is None:
            path = Path(self.template_path)
            if not path.exists():
                raise FileNotFoundError(f"Email template not found: {self.template_path}")
            self._template = path.read_text(encoding="utf-8")

        lines = self._template.strip().split("\n")
        subject = ""
        body_start = 0

        for i, line in enumerate(lines):
            if line.lower().startswith("subject:"):
                subject = line.split(":", 1)[1].strip()
                body_start = i + 1
                break

        while body_start < len(lines) and not lines[body_start].strip():
            body_start += 1

        body = "\n".join(lines[body_start:])

        html_body = None
        html_path = Path(self.template_path).with_suffix(".html")
        if html_path.exists():
            html_body = html_path.read_text(encoding="utf-8")

        return subject, body, html_body

    def _render_template(
        self,
        subject: str,
        body: str,
        name: str,
        business_name: str,
        niche: str,
        personalized_pitch: str = "",
        html_body: str | None = None,
    ) -> tuple[str, str, str | None]:
        """Replace placeholders in template. Returns (subject, plain, html)."""
        replacements = {
            "{{Name}}": name,
            "{{Business Name}}": business_name,
            "{{niche}}": niche,
            "{{personalized_pitch}}": personalized_pitch,
        }
        rendered_subject = subject
        rendered_body = body
        rendered_html = html_body
        for placeholder, value in replacements.items():
            rendered_subject = rendered_subject.replace(placeholder, value)
            rendered_body = rendered_body.replace(placeholder, value)
            if rendered_html:
                rendered_html = rendered_html.replace(placeholder, value)
        return rendered_subject, rendered_body, rendered_html

    def _extract_owner_name(self, business_name: str) -> str:
        parts = business_name.split()
        if len(parts) >= 2 and parts[0] in ("Dr", "Dr."):
            return parts[1]
        return "there"

    def _parse_facts(
        self,
        reasons: str,
        website: str,
        ai_score: int | None = None,
        ai_design_score: int | None = None,
        ai_mobile_score: int | None = None,
        ai_professionalism_score: int | None = None,
        ai_cta_score: int | None = None,
        ai_summary: str = "",
        ai_issues: list[str] | None = None,
    ) -> dict:
        """Parse qualification reasons + AI scorer data into structured facts."""
        r = reasons.lower() if reasons else ""
        has_website = bool(website and website.strip())

        facts = {
            "has_website": has_website,
            "no_website": "no website" in r and not has_website,
            "social_media_only": "social media only" in r or "social only" in r,
            "outdated_design": "outdated" in r,
            "not_mobile_friendly": "not mobile" in r or "mobile-friendly" in r,
            "slow_loading": "slow" in r,
            "broken_layout": "broken" in r or "poor design" in r,
            "uses_free_email": "free email" in r or "gmail" in r or "yahoo" in r,
            "website_unreachable": "unreachable" in r or "empty website" in r,
            "ai_poor_design": False,
            "ai_score": ai_score,
            "ai_design_score": ai_design_score,
            "ai_mobile_score": ai_mobile_score,
            "ai_professionalism_score": ai_professionalism_score,
            "ai_cta_score": ai_cta_score,
            "ai_summary": ai_summary or "",
            "ai_issues": ai_issues or [],
        }

        if ai_score is not None and ai_score <= 5:
            facts["ai_poor_design"] = True
        elif "ai:" in r and ("poor design" in r or "mediocre" in r):
            facts["ai_poor_design"] = True

        if ai_mobile_score is not None and ai_mobile_score <= 4:
            facts["not_mobile_friendly"] = True
        if ai_design_score is not None and ai_design_score <= 4:
            facts["outdated_design"] = True

        return facts

    def _build_fact_sheet(self, facts: dict, reasons: str) -> str:
        """Build a human-readable fact sheet from verified data."""
        lines = []
        lines.append(f"- has_website: {facts['has_website']}")

        if facts["no_website"]:
            lines.append("- no_website: TRUE — business has no website at all")
        if facts["social_media_only"]:
            lines.append("- social_media_only: TRUE — website URL points to a social media page (Facebook, Instagram, etc.), not a real business website")
        if facts["outdated_design"]:
            lines.append("- outdated_design: TRUE — website uses outdated HTML patterns or has an old copyright year")
        if facts["not_mobile_friendly"]:
            lines.append("- not_mobile_friendly: TRUE — website does not have a mobile viewport tag, won't display well on phones")
        if facts["slow_loading"]:
            lines.append("- slow_loading: TRUE — website took over 4 seconds to load")
        if facts["broken_layout"]:
            lines.append("- broken_layout: TRUE — website has broken images, missing stylesheets, or layout issues")
        if facts["uses_free_email"]:
            lines.append("- uses_free_email: TRUE — business uses Gmail/Yahoo/Hotmail instead of a branded email")
        if facts["website_unreachable"]:
            lines.append("- website_unreachable: TRUE — website could not be loaded at all (broken/down)")

        has_ai = facts["ai_score"] is not None
        if has_ai:
            lines.append("")
            lines.append("AI VISUAL ANALYSIS RESULTS (verified by screenshot inspection):")
            lines.append(f"- overall_score: {facts['ai_score']}/10")
            if facts["ai_design_score"] is not None:
                lines.append(f"- design_score: {facts['ai_design_score']}/10")
            if facts["ai_mobile_score"] is not None:
                lines.append(f"- mobile_score: {facts['ai_mobile_score']}/10")
            if facts["ai_professionalism_score"] is not None:
                lines.append(f"- professionalism_score: {facts['ai_professionalism_score']}/10")
            if facts["ai_cta_score"] is not None:
                lines.append(f"- cta_score: {facts['ai_cta_score']}/10")
            if facts["ai_summary"]:
                lines.append(f"- ai_verdict: {facts['ai_summary']}")
            if facts["ai_issues"]:
                lines.append(f"- specific_issues_found: {', '.join(facts['ai_issues'])}")
            lines.append("(You may reference these AI findings — they are verified from a real screenshot)")
        elif facts["ai_poor_design"]:
            lines.append("- ai_poor_design: TRUE — AI visual analysis rated the design as poor or mediocre")

        if not has_ai and not any(facts[k] for k in ("no_website", "social_media_only", "outdated_design",
                "not_mobile_friendly", "slow_loading", "broken_layout", "uses_free_email",
                "website_unreachable", "ai_poor_design")):
            lines.append(f"- raw_reasons: {reasons}")

        return "\n".join(lines)

    def _validate_pitch(self, pitch: str, facts: dict[str, bool]) -> str:
        """Check AI output for contradictions against verified facts and fix them."""
        pitch_lower = pitch.lower()

        if facts["has_website"] and not facts["no_website"] and not facts["social_media_only"]:
            no_website_phrases = [
                "don't have a website", "do not have a website",
                "doesn't have a website", "does not have a website",
                "no website", "without a website",
                "lack a website", "lacking a website",
                "haven't got a website", "have no website",
                "no online presence", "no web presence",
                "invisible online", "invisible to",
                "can't find you online", "cannot find you online",
                "can't be found online", "cannot be found online",
                "doesn't exist online", "does not exist online",
                "no digital presence", "missing a website",
            ]
            for phrase in no_website_phrases:
                if phrase in pitch_lower:
                    logger.warning(
                        f"AI pitch falsely claims no website — falling back to rules"
                    )
                    return ""

        if not facts["outdated_design"] and not facts["ai_poor_design"]:
            outdated_phrases = [
                "looks outdated", "looks dated", "looks old",
                "design is outdated", "design is dated",
                "looks like it was built in", "looks like it hasn't been updated",
            ]
            for phrase in outdated_phrases:
                if phrase in pitch_lower:
                    logger.warning(
                        f"AI pitch falsely claims outdated design — falling back to rules"
                    )
                    return ""

        if not facts["slow_loading"]:
            slow_phrases = ["takes a long time to load", "slow to load", "slow loading"]
            for phrase in slow_phrases:
                if phrase in pitch_lower:
                    logger.warning(
                        f"AI pitch falsely claims slow loading — falling back to rules"
                    )
                    return ""

        if not facts["not_mobile_friendly"]:
            mobile_phrases = [
                "not mobile friendly", "not mobile-friendly",
                "doesn't work on mobile", "does not work on mobile",
                "not optimized for mobile", "not responsive",
            ]
            for phrase in mobile_phrases:
                if phrase in pitch_lower:
                    logger.warning(
                        f"AI pitch falsely claims not mobile-friendly — falling back to rules"
                    )
                    return ""

        return pitch

    async def _generate_personalized_pitch(
        self,
        business_name: str,
        niche: str,
        city: str,
        website: str,
        qualification_reasons: str,
        ai_score: int | None = None,
        ai_design_score: int | None = None,
        ai_mobile_score: int | None = None,
        ai_professionalism_score: int | None = None,
        ai_cta_score: int | None = None,
        ai_summary: str = "",
        ai_issues: list[str] | None = None,
    ) -> str:
        """Use AI to generate a personalized pitch based on verified facts only."""
        facts = self._parse_facts(
            qualification_reasons, website,
            ai_score=ai_score,
            ai_design_score=ai_design_score,
            ai_mobile_score=ai_mobile_score,
            ai_professionalism_score=ai_professionalism_score,
            ai_cta_score=ai_cta_score,
            ai_summary=ai_summary,
            ai_issues=ai_issues,
        )
        fact_sheet = self._build_fact_sheet(facts, qualification_reasons)

        client = self._get_ai_client()
        if client is None:
            return self._fallback_pitch(qualification_reasons, niche, facts)

        prompt = PERSONALIZATION_PROMPT.format(
            fact_sheet=fact_sheet,
            business_name=business_name,
            niche=niche,
            city=city or "their area",
        )

        try:
            response = await client.chat.completions.create(
                model=self.ai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0.7,
            )
            pitch = response.choices[0].message.content.strip()
            pitch = pitch.strip('"\'')

            validated = self._validate_pitch(pitch, facts)
            if not validated:
                logger.info(f"AI pitch for {business_name} failed validation, using fallback")
                return self._fallback_pitch(qualification_reasons, niche, facts)

            logger.debug(f"AI pitch for {business_name}: {validated[:80]}...")
            return validated
        except Exception as e:
            logger.warning(f"AI personalization failed for {business_name}: {e}")
            return self._fallback_pitch(qualification_reasons, niche, facts)

    def _fallback_pitch(self, reasons: str, niche: str, facts: dict[str, bool] | None = None) -> str:
        """Rule-based fallback that only states verified facts."""
        if facts is None:
            facts = self._parse_facts(reasons, "")

        if facts["no_website"]:
            return (
                f"Right now, when potential customers search for {niche} services in your area, "
                f"they can't find you online. Studies show over 80% of people research businesses "
                f"online before visiting — without a website, you're invisible to most of them. "
                f"A clean, professional site could change that overnight."
            )
        if facts["social_media_only"]:
            return (
                f"Your social media page is a great start, but it doesn't show up in Google searches "
                f"the way a proper website does. Most customers looking for {niche} services "
                f"start with a Google search — a dedicated website would help you capture "
                f"that traffic and convert visitors into real bookings."
            )
        if facts["website_unreachable"]:
            return (
                f"When I tried to visit your website, it wasn't loading — which means potential "
                f"customers are running into the same issue. For a {niche} business, every hour "
                f"your site is down is a missed opportunity. A reliable, well-built website "
                f"ensures you're always reachable."
            )
        if facts["outdated_design"]:
            return (
                f"Your current website has good bones, but the design looks like it could use "
                f"a refresh. In today's market, customers judge a {niche} business within seconds "
                f"of landing on the site — an updated look can make the difference between "
                f"someone booking or clicking away."
            )
        if facts["not_mobile_friendly"]:
            return (
                f"Your website doesn't appear to be optimized for mobile devices. "
                f"Over 60% of local searches happen on phones — if your site is hard to "
                f"navigate on a phone, those visitors are likely bouncing to competitors "
                f"with mobile-friendly sites."
            )
        if facts["slow_loading"]:
            return (
                f"Your website takes a bit longer than ideal to load, and research shows "
                f"most visitors leave a site that doesn't load within 3 seconds. For a {niche} "
                f"business, every lost visitor is a lost booking or inquiry. A faster site "
                f"would help you keep those potential customers engaged."
            )
        if facts["broken_layout"]:
            return (
                f"Your website has some layout issues that might be affecting how customers "
                f"perceive your business. First impressions matter — when someone visits a "
                f"{niche} site and things look off, they often leave without reaching out. "
                f"A polished, well-structured design would help build that instant trust."
            )
        if facts["uses_free_email"]:
            if facts["has_website"]:
                return (
                    f"One thing that stood out is that you're using a free email provider like Gmail "
                    f"for business inquiries. While your website is there, pairing it with a professional "
                    f"email (like info@yourbusiness.com) would help build credibility. Customers "
                    f"tend to trust {niche} businesses more when everything looks polished and consistent."
                )
            return (
                f"I noticed you're using a free email provider, which is common but can make "
                f"a {niche} business look less established. A professional email tied to your "
                f"own domain (like info@yourbusiness.com) paired with a matching website "
                f"builds credibility and trust with new customers."
            )
        if facts["ai_poor_design"]:
            return (
                f"After looking at your website, I think there's a real opportunity to elevate "
                f"how your business comes across online. A more modern, polished design could help "
                f"you stand out from other {niche} businesses in the area and give potential "
                f"customers more confidence to reach out."
            )
        return (
            f"Looking at your current online presence, there's a real opportunity to attract "
            f"more customers with a stronger website. In the {niche} space, "
            f"a polished online presence is often the difference between getting the call "
            f"or losing it to a competitor."
        )

    async def send_email(
        self,
        to_email: str,
        business_name: str,
        niche: str,
        contact_name: str | None = None,
        website: str = "",
        city: str = "",
        qualification_reasons: str = "",
        ai_score: int | None = None,
        ai_design_score: int | None = None,
        ai_mobile_score: int | None = None,
        ai_professionalism_score: int | None = None,
        ai_cta_score: int | None = None,
        ai_summary: str = "",
        ai_issues: list[str] | None = None,
    ) -> dict:
        """Send a single outreach email with AI-personalized pitch."""
        if self._sent_today >= self.daily_limit:
            logger.warning(f"Daily email limit reached ({self.daily_limit})")
            return {"error": "daily_limit_reached"}

        if not self.smtp_password:
            logger.error("SMTP password not configured")
            return {"error": "no_smtp_password"}

        name = contact_name or self._extract_owner_name(business_name)

        pitch = await self._generate_personalized_pitch(
            business_name=business_name,
            niche=niche,
            city=city,
            website=website,
            qualification_reasons=qualification_reasons,
            ai_score=ai_score,
            ai_design_score=ai_design_score,
            ai_mobile_score=ai_mobile_score,
            ai_professionalism_score=ai_professionalism_score,
            ai_cta_score=ai_cta_score,
            ai_summary=ai_summary,
            ai_issues=ai_issues,
        )

        subject_tpl, body_tpl, html_tpl = self._load_template()
        subject, body, html_body = self._render_template(
            subject_tpl, body_tpl, name, business_name, niche, pitch, html_tpl
        )

        try:
            msg = MIMEMultipart("alternative") if html_body else MIMEText(body)
            if html_body:
                msg.attach(MIMEText(body, "plain"))
                msg.attach(MIMEText(html_body, "html"))

            msg["From"] = f"{self.sender_name} <{self.sender_email}>"
            msg["To"] = to_email
            msg["Subject"] = subject

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.smtp_password)
                server.sendmail(self.sender_email, to_email, msg.as_string())

            self._sent_today += 1
            logger.info(f"Email sent to {to_email} ({business_name}) [{self._sent_today}/{self.daily_limit}]")
            return {"id": f"smtp-{self._sent_today}"}
        except Exception as e:
            logger.error(f"Failed to send email to {to_email}: {e}")
            return {"error": str(e)}

    async def send_batch(
        self,
        leads: list[dict],
    ) -> list[dict]:
        """Send emails to a batch of leads with rate limiting.

        Each lead dict should have: email, business_name, niche,
        and optionally: contact_name, website, city, qualification_reasons
        """
        results = []
        for i, lead in enumerate(leads):
            if self._sent_today >= self.daily_limit:
                logger.warning(f"Daily limit reached. {len(leads) - i} emails remaining for next run.")
                break

            email = lead.get("email") or lead.get("Contact Email", "")
            if not email or "@" not in email:
                logger.debug(f"Skipping {lead.get('business_name', '?')}: no valid email")
                results.append({"error": "no_email"})
                continue

            result = await self.send_email(
                to_email=email,
                business_name=lead.get("business_name") or lead.get("Business Name", ""),
                niche=lead.get("niche") or lead.get("Niche", ""),
                contact_name=lead.get("contact_name"),
                website=lead.get("website") or lead.get("Website", ""),
                city=lead.get("city") or lead.get("City", ""),
                qualification_reasons=lead.get("qualification_reasons") or lead.get("Qualification Reasons", ""),
                ai_score=lead.get("ai_score"),
                ai_design_score=lead.get("ai_design_score"),
                ai_mobile_score=lead.get("ai_mobile_score"),
                ai_professionalism_score=lead.get("ai_professionalism_score"),
                ai_cta_score=lead.get("ai_cta_score"),
                ai_summary=lead.get("ai_summary", ""),
                ai_issues=lead.get("ai_issues"),
            )
            results.append(result)

            if i < len(leads) - 1:
                delay = random.randint(self.delay_min, self.delay_max)
                logger.debug(f"Waiting {delay}s before next email...")
                await asyncio.sleep(delay)

        sent = sum(1 for r in results if "error" not in r)
        logger.info(f"Email batch complete: {sent}/{len(leads)} sent successfully")
        return results

    @property
    def remaining_today(self) -> int:
        return max(0, self.daily_limit - self._sent_today)

    def reset_daily_counter(self) -> None:
        self._sent_today = 0
