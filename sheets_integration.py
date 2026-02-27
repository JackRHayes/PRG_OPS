"""
MODULE — Google Sheets Integration
Connects to Google Sheets via OAuth, syncs job data automatically.
"""
import os
import json
import logging
from typing import Optional, Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import Flow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
TOKEN_FILE = 'token.json'
CREDENTIALS_FILE = 'credentials.json'


class GoogleSheetsConnector:
    """Manages Google Sheets OAuth and data fetching."""

    def __init__(self):
        self.creds = None
        self.service = None
        self.load_credentials()

    def load_credentials(self):
        """Load saved credentials if they exist."""
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, 'r') as token:
                self.creds = Credentials.from_authorized_user_info(json.load(token), SCOPES)

        # Refresh if expired
        if self.creds and self.creds.expired and self.creds.refresh_token:
            self.creds.refresh(Request())
            self.save_credentials()

        if self.creds and self.creds.valid:
            self.service = build('sheets', 'v4', credentials=self.creds)

    def save_credentials(self):
        """Save credentials to disk."""
        with open(TOKEN_FILE, 'w') as token:
            token.write(self.creds.to_json())

    def is_authenticated(self) -> bool:
        """Check if user is authenticated."""
        return self.creds is not None and self.creds.valid

    def get_auth_url(self, redirect_uri: str) -> str:
        """Generate OAuth authorization URL."""
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"{CREDENTIALS_FILE} not found")

        flow = Flow.from_client_secrets_file(
            CREDENTIALS_FILE,
            scopes=SCOPES,
            redirect_uri=redirect_uri
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        return auth_url

    def handle_oauth_callback(self, code: str, redirect_uri: str) -> bool:
        """Exchange auth code for credentials."""
        try:
            flow = Flow.from_client_secrets_file(
                CREDENTIALS_FILE,
                scopes=SCOPES,
                redirect_uri=redirect_uri
            )
            flow.fetch_token(code=code)
            self.creds = flow.credentials
            self.save_credentials()
            self.service = build('sheets', 'v4', credentials=self.creds)
            return True
        except Exception as e:
            logger.error(f"[SHEETS] OAuth error: {e}")
            return False

    def get_spreadsheet_info(self, spreadsheet_id: str) -> Optional[Dict]:
        """Get spreadsheet metadata (title, sheet names)."""
        if not self.service:
            return None
        try:
            result = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            return {
                'title': result.get('properties', {}).get('title', 'Untitled'),
                'sheets': [s['properties']['title'] for s in result.get('sheets', [])]
            }
        except HttpError as e:
            logger.error(f"[SHEETS] Error fetching spreadsheet: {e}")
            raise

    def read_sheet(self, spreadsheet_id: str, range_name: str) -> Optional[List[List]]:
        """Read data from a specific sheet range."""
        if not self.service:
            return None
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            return result.get('values', [])
        except HttpError as e:
            logger.error(f"[SHEETS] Error reading sheet: {e}")
            return None

    def disconnect(self):
        """Remove stored credentials."""
        if os.path.exists(TOKEN_FILE):
            os.remove(TOKEN_FILE)
        self.creds = None
        self.service = None


def extract_spreadsheet_id(url: str) -> Optional[str]:
    """Extract spreadsheet ID from Google Sheets URL."""
    # URL format: https://docs.google.com/spreadsheets/d/{ID}/edit...
    if '/spreadsheets/d/' in url:
        parts = url.split('/spreadsheets/d/')[1].split('/')
        return parts[0] if parts else None
    return None


def convert_sheet_to_jobs(rows: List[List], column_mapping: Dict[str, int]) -> List[Dict]:
    """
    Convert raw sheet rows to job dictionaries using column mapping.

    column_mapping format: {'job_id': 0, 'contractor': 1, ...}
    where values are column indices.
    """
    if not rows or len(rows) < 2:
        return []

    # Skip header row
    data_rows = rows[1:]
    jobs = []

    for row in data_rows:
        # Pad row with empty strings if needed
        while len(row) < max(column_mapping.values()) + 1:
            row.append('')

        job = {}
        for field, col_idx in column_mapping.items():
            job[field] = row[col_idx].strip() if col_idx < len(row) else ''

        jobs.append(job)

    return jobs


def create_default_column_mapping(headers: List[str]) -> Dict[str, Optional[int]]:
    """
    Auto-detect column mapping based on header names.
    Returns mapping of required fields to column indices (or None if not found).
    """
    required_fields = [
        'job_id', 'contractor', 'scope_type', 'region',
        'start_date', 'planned_end_date', 'actual_end_date',
        'status', 'markout_required', 'markout_issues',
        'inspections_failed', 'crew_type'
    ]

    # Normalize headers for matching
    normalized = [h.lower().strip().replace(' ', '_').replace('-', '_') for h in headers]

    mapping = {}
    for field in required_fields:
        # Try exact match
        if field in normalized:
            mapping[field] = normalized.index(field)
        # Try partial matches
        elif any(field in h for h in normalized):
            for i, h in enumerate(normalized):
                if field in h:
                    mapping[field] = i
                    break
        else:
            mapping[field] = None

    return mapping


# Singleton instance
_connector = None

def get_connector() -> GoogleSheetsConnector:
    """Get or create the global connector instance."""
    if not GOOGLE_AVAILABLE:
        raise RuntimeError(
            "Google API libraries not installed. "
            "Run: pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client"
        )
    global _connector
    if _connector is None:
        _connector = GoogleSheetsConnector()
    return _connector
