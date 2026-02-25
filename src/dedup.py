import logging
from src.models import Business, QualifiedLead
from src.utils import normalize_phone, normalize_name

logger = logging.getLogger("leadgen.dedup")


class DeduplicationEngine:
    def __init__(self):
        self._phone_index: set[str] = set()
        self._name_city_index: set[str] = set()
        self._loaded = False

    def load_existing_leads(self, existing_records: list[dict]) -> None:
        """Build dedup indexes from existing tracker records."""
        self._phone_index.clear()
        self._name_city_index.clear()

        for record in existing_records:
            phone = normalize_phone(str(record.get("Phone", "")))
            if phone and len(phone) >= 7:
                self._phone_index.add(phone)

            name = normalize_name(str(record.get("Business Name", "")))
            city = normalize_name(str(record.get("City", "")))
            if name and city:
                self._name_city_index.add(f"{name}|{city}")

        self._loaded = True
        logger.info(
            f"Dedup index loaded: {len(self._phone_index)} phones, "
            f"{len(self._name_city_index)} name+city combos"
        )

    def is_duplicate(self, business: Business) -> bool:
        """Check if a business already exists in the tracker."""
        phone = normalize_phone(business.phone or "")
        if phone and len(phone) >= 7 and phone in self._phone_index:
            logger.debug(f"Duplicate found (phone): {business.name}")
            return True

        name = normalize_name(business.name)
        city = normalize_name(business.city)
        key = f"{name}|{city}"
        if key in self._name_city_index:
            logger.debug(f"Duplicate found (name+city): {business.name}")
            return True

        return False

    def register(self, business: Business) -> None:
        """Register a business in the dedup index to prevent future duplicates within this batch."""
        phone = normalize_phone(business.phone or "")
        if phone and len(phone) >= 7:
            self._phone_index.add(phone)

        name = normalize_name(business.name)
        city = normalize_name(business.city)
        if name and city:
            self._name_city_index.add(f"{name}|{city}")

    def filter_duplicates(self, businesses: list[Business]) -> list[Business]:
        """Remove duplicates from a list of businesses, also deduping within the batch."""
        unique = []
        for biz in businesses:
            if not self.is_duplicate(biz):
                unique.append(biz)
                self.register(biz)
            else:
                logger.debug(f"Filtered duplicate: {biz.name}")

        filtered = len(businesses) - len(unique)
        if filtered > 0:
            logger.info(f"Removed {filtered} duplicates, {len(unique)} unique leads remain")
        return unique
