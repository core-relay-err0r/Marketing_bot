import json
import logging
import os
import time
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

from src.models import QualifiedLead

logger = logging.getLogger("leadgen.sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MAX_RETRIES = 5
RETRY_DELAY = 10


class SheetsManager:
    def __init__(
        self,
        spreadsheet_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
    ):
        self.spreadsheet_id = spreadsheet_id or os.getenv("GOOGLE_SHEETS_ID", "")
        self.credentials_path = credentials_path or os.getenv(
            "GOOGLE_CREDENTIALS_PATH", "config/google_credentials.json"
        )
        self._client: Optional[gspread.Client] = None
        self._spreadsheet: Optional[gspread.Spreadsheet] = None

    def _connect(self) -> None:
        if self._client is not None and self._spreadsheet is not None:
            return

        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if creds_json:
            info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        elif os.path.exists(self.credentials_path):
            creds = Credentials.from_service_account_file(
                self.credentials_path, scopes=SCOPES
            )
        else:
            raise FileNotFoundError(
                f"Google credentials not found. Set GOOGLE_CREDENTIALS_JSON env var "
                f"or place the JSON key file at {self.credentials_path}."
            )

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._client = gspread.authorize(creds)
                self._spreadsheet = self._client.open_by_key(self.spreadsheet_id)
                logger.info(f"Connected to spreadsheet: {self._spreadsheet.title}")
                return
            except Exception as e:
                self._client = None
                self._spreadsheet = None
                if attempt < MAX_RETRIES:
                    wait = RETRY_DELAY * attempt
                    logger.warning(
                        f"Sheets connection attempt {attempt}/{MAX_RETRIES} failed: {e}. "
                        f"Retrying in {wait}s..."
                    )
                    time.sleep(wait)
                else:
                    raise ConnectionError(
                        f"Failed to connect to Google Sheets after {MAX_RETRIES} attempts: {e}"
                    ) from e

    def get_or_create_worksheet(self, title: str) -> gspread.Worksheet:
        """Get existing worksheet or create a new one with headers."""
        self._connect()
        try:
            ws = self._spreadsheet.worksheet(title)
            logger.info(f"Using existing worksheet: {title}")
        except gspread.WorksheetNotFound:
            ws = self._spreadsheet.add_worksheet(title=title, rows=1000, cols=20)
            ws.append_row(QualifiedLead.sheet_headers())
            logger.info(f"Created new worksheet: {title}")
        return ws

    def get_all_leads(self, worksheet_title: str) -> list[dict]:
        """Read all leads from a worksheet as list of dicts."""
        self._connect()
        try:
            ws = self._spreadsheet.worksheet(worksheet_title)
            headers = ws.row_values(1)
            # Deduplicate empty/blank headers by giving them unique names
            seen = {}
            clean_headers = []
            for h in headers:
                h = h.strip()
                if not h:
                    h = f"_empty_{len(seen)}"
                if h in seen:
                    seen[h] += 1
                    h = f"{h}_{seen[h]}"
                else:
                    seen[h] = 0
                clean_headers.append(h)
            records = ws.get_all_records(expected_headers=clean_headers)
            # Remove any records with placeholder headers
            for rec in records:
                for key in list(rec.keys()):
                    if key.startswith("_empty_"):
                        del rec[key]
            logger.info(f"Read {len(records)} existing leads from '{worksheet_title}'")
            return records
        except gspread.WorksheetNotFound:
            logger.info(f"Worksheet '{worksheet_title}' not found, returning empty")
            return []

    def _safe_get_records(self, ws) -> list[dict]:
        """Read records from a worksheet, handling duplicate/empty headers."""
        headers = ws.row_values(1)
        seen = {}
        clean_headers = []
        for h in headers:
            h = h.strip()
            if not h:
                h = f"_empty_{len(seen)}"
            if h in seen:
                seen[h] += 1
                h = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
            clean_headers.append(h)
        records = ws.get_all_records(expected_headers=clean_headers)
        for rec in records:
            for key in list(rec.keys()):
                if key.startswith("_empty_"):
                    del rec[key]
        return records

    def get_all_leads_all_sheets(self) -> list[dict]:
        """Read leads from ALL worksheets for comprehensive dedup."""
        self._connect()
        all_leads = []
        for ws in self._spreadsheet.worksheets():
            try:
                records = self._safe_get_records(ws)
                all_leads.extend(records)
            except Exception as e:
                logger.debug(f"Could not read worksheet '{ws.title}': {e}")
        logger.info(f"Read {len(all_leads)} total leads across all worksheets")
        return all_leads

    def append_leads(self, worksheet_title: str, leads: list[QualifiedLead]) -> int:
        """Append qualified leads to the worksheet. Returns count of rows added."""
        if not leads:
            return 0

        ws = self.get_or_create_worksheet(worksheet_title)

        rows = [lead.to_row() for lead in leads]
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        logger.info(f"Appended {len(rows)} leads to '{worksheet_title}'")
        return len(rows)

    def mark_email_sent(self, worksheet_title: str, row_index: int, sent_at: str) -> None:
        """Update the Email Sent and Email Sent At columns for a lead."""
        self._connect()
        ws = self._spreadsheet.worksheet(worksheet_title)
        headers = ws.row_values(1)

        try:
            sent_col = headers.index("Email Sent") + 1
            sent_at_col = headers.index("Email Sent At") + 1
            ws.update_cell(row_index, sent_col, "Yes")
            ws.update_cell(row_index, sent_at_col, sent_at)
        except ValueError:
            logger.warning("Could not find Email Sent columns in worksheet")

    def get_unsent_leads(self, worksheet_title: str) -> list[tuple[int, dict]]:
        """Get leads that haven't been emailed yet. Returns (row_index, lead_dict) tuples."""
        self._connect()
        try:
            ws = self._spreadsheet.worksheet(worksheet_title)
        except gspread.WorksheetNotFound:
            return []

        records = self._safe_get_records(ws)
        unsent = []
        for i, record in enumerate(records):
            if record.get("Email Sent", "No") != "Yes":
                unsent.append((i + 2, record))  # +2 for header row and 1-indexed
        logger.info(f"Found {len(unsent)} unsent leads in '{worksheet_title}'")
        return unsent

    def get_daily_stats(self, worksheet_title: str) -> dict:
        """Get stats for a worksheet."""
        self._connect()
        try:
            ws = self._spreadsheet.worksheet(worksheet_title)
            records = ws.get_all_records()
        except gspread.WorksheetNotFound:
            return {"total": 0, "emailed": 0, "pending_email": 0}

        total = len(records)
        emailed = sum(1 for r in records if r.get("Email Sent") == "Yes")
        return {
            "total": total,
            "emailed": emailed,
            "pending_email": total - emailed,
        }
