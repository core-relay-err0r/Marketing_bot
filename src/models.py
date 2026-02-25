from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional


@dataclass
class Business:
    """Raw scraped business from Google Maps."""
    name: str
    address: str
    city: str
    country: str
    phone: Optional[str] = None
    website: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    category: Optional[str] = None
    niche: str = ""
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class QualificationResult:
    """Result of qualifying a single business website."""
    qualifies: bool = False
    reasons: list[str] = field(default_factory=list)
    disqualify_reason: Optional[str] = None
    has_website: bool = False
    is_mobile_friendly: bool = True
    load_time_seconds: Optional[float] = None
    is_slow: bool = False
    is_outdated: bool = False
    is_social_only: bool = False
    uses_free_email: bool = False
    has_broken_layout: bool = False
    is_chain_or_franchise: bool = False
    contact_email: Optional[str] = None
    ai_score: Optional[int] = None
    ai_design_score: Optional[int] = None
    ai_mobile_score: Optional[int] = None
    ai_professionalism_score: Optional[int] = None
    ai_cta_score: Optional[int] = None
    ai_summary: Optional[str] = None
    ai_issues: list[str] = field(default_factory=list)


@dataclass
class QualifiedLead:
    """A business that passed qualification checks."""
    name: str
    address: str
    city: str
    country: str
    niche: str
    phone: Optional[str] = None
    website: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    contact_email: Optional[str] = None
    qualification_reasons: str = ""
    ai_score: Optional[int] = None
    ai_design_score: Optional[int] = None
    ai_mobile_score: Optional[int] = None
    ai_professionalism_score: Optional[int] = None
    ai_cta_score: Optional[int] = None
    ai_summary: str = ""
    ai_issues: list[str] = field(default_factory=list)
    email_sent: bool = False
    email_sent_at: Optional[str] = None
    added_by: str = "Thar"
    added_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_row(self) -> list[str]:
        """Convert to a row for Google Sheets."""
        return [
            self.name,
            self.address,
            self.city,
            self.country,
            self.niche,
            self.phone or "",
            self.website or "",
            str(self.rating) if self.rating else "",
            str(self.review_count) if self.review_count else "",
            self.contact_email or "",
            self.qualification_reasons,
            "Yes" if self.email_sent else "No",
            self.email_sent_at or "",
            self.added_by,
            self.added_at,
            str(self.ai_score) + "/10" if self.ai_score is not None else "",
            self.ai_summary,
        ]

    @staticmethod
    def sheet_headers() -> list[str]:
        return [
            "Business Name",
            "Address",
            "City",
            "Country",
            "Niche",
            "Phone",
            "Website",
            "Rating",
            "Review Count",
            "Contact Email",
            "Qualification Reasons",
            "Email Sent",
            "Email Sent At",
            "Added By",
            "Added At",
            "AI Score",
            "AI Summary",
        ]

    def to_dict(self) -> dict:
        return asdict(self)
