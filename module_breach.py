import requests
import time
import re
import urllib.parse
import pandas as pd
from typing import Dict, List, Optional, Tuple


def _normalize_name_part(part: str) -> str:
    replacements = {
        'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss',
        'Ä': 'ae', 'Ö': 'oe', 'Ü': 'ue',
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
        'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u',
        'ý': 'y', 'ñ': 'n', 'ç': 'c',
    }
    part = part.lower()
    for char, replacement in replacements.items():
        part = part.replace(char, replacement)
    return re.sub(r'[^a-z]', '', part)


def generate_email_candidate(full_name: str, domain: str) -> Optional[str]:
    if not full_name or not domain:
        return None

    name = re.sub(
        r'\b(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|ing\.?|dipl\.?|b\.?sc\.?|m\.?sc\.?)\b',
        '', full_name, flags=re.IGNORECASE,
    ).strip()
    name = re.split(r'\s*[-–|@]\s*', name)[0].strip()
    name = re.sub(r'\s*\(.*?\)', '', name).strip()

    parts = [p for p in name.split() if len(p) > 1]
    if len(parts) < 2:
        return None

    first = _normalize_name_part(parts[0])
    last  = _normalize_name_part(parts[-1])

    if not first or not last:
        return None

    return f"{first}.{last}@{domain.lower()}"


class BreachChecker:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.base_url = "https://haveibeenpwned.com/api/v3"
        self.unified_search_url = "https://haveibeenpwned.com:443/unifiedsearch/"
        self.headers = {
            'hibp-api-key': api_key,
            'user-agent': 'IDP-Student-Project',
        }
        self.browser_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/100.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://haveibeenpwned.com/",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Te": "trailers",
        }
        self.sleep_interval = 2

    def _check_hibp_original(self, account: str) -> bool:
        search_url = self.unified_search_url + urllib.parse.quote(account.strip())
        time.sleep(self.sleep_interval)
        response = requests.get(search_url, headers=self.browser_headers)
        # parse_request() from original: 200 → True, else → False
        return response.status_code == 200

    def _check_email_web(self, email: str) -> Dict:
        print(f"🌐 Using web method for {email}...")
        try:
            is_pwned = self._check_hibp_original(email)
            if is_pwned:
                return {
                    "status":  "leaked",
                    "count":   1,           # no count available without API
                    "details": [],          # no breach names available without API
                    "method":  "web",
                }
            else:
                return {"status": "safe", "count": 0, "method": "web"}
        except Exception as e:
            return {"status": "error", "message": str(e), "method": "web"}

    def _check_email_api(self, email: str) -> Dict:
        print(f"🔑 Using API method for {email}...")
        url = f"{self.base_url}/breachedaccount/{email}?truncateResponse=false"
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                leaks = response.json()
                return {
                    "status":  "leaked",
                    "count":   len(leaks),
                    "details": [l['Name'] for l in leaks],
                    "method":  "api",
                }
            elif response.status_code == 404:
                return {"status": "safe", "count": 0, "method": "api"}
            elif response.status_code == 429:
                print("⚠️ Rate limit hit. Waiting 2s...")
                time.sleep(2)
                return self._check_email_api(email)
            else:
                return {"status": "error", "code": response.status_code, "method": "api"}
        except Exception as e:
            return {"status": "error", "message": str(e), "method": "api"}

    def check_email(self, email: str, use_api_if_available: bool = True) -> Dict:
        if use_api_if_available and self.api_key:
            return self._check_email_api(email)
        else:
            return self._check_email_web(email)

    def check_hibp_api(self, email: str):
        url = f"https://haveibeenpwned.com/api/v3/breachedaccount/{email}"
        headers = {"hibp-api-key": self.api_key, "user-agent": "OSINT-Project"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return []
        return None

    def batch_check(
        self,
        accounts: List[str],
        use_api_if_available: bool = True,
        max_workers: int = 1,
    ) -> Dict:
        results = {}
        for account in accounts:
            results[account] = self.check_email(account, use_api_if_available)
            time.sleep(1)
        return results

    # Generate firstname.lastname@domain and check
    def generate_and_check_emails(
        self,
        df_people: pd.DataFrame,
        domain: str,
        use_api_if_available: bool = False,
        streamlit_ui=None,
    ) -> Dict[str, Dict]:
        st = streamlit_ui

        if df_people.empty:
            if st:
                st.info("ℹ️ No people data available for email generation.")
            return {}

        name_col = next(
            (c for c in ("Real_Name", "Name") if c in df_people.columns), None
        )
        if not name_col:
            if st:
                st.warning("⚠️ No name column found in people data.")
            return {}

        candidates: List[tuple] = []
        seen_emails: set = set()

        for raw_name in df_people[name_col].dropna():
            name = str(raw_name).strip()
            if not name or name in ("Unknown", "N/A", ""):
                continue
            email = generate_email_candidate(name, domain)
            if not email or email in seen_emails:
                continue
            seen_emails.add(email)
            candidates.append((name, email))

        if not candidates:
            if st:
                st.info("ℹ️ Could not derive any email candidates from the people list.")
            return {}

        if st:
            st.info(
                f"📧 Generated {len(candidates)} candidate email(s) — "
                f"checking against HIBP..."
            )

        results: Dict[str, Dict] = {}
        progress = st.progress(0, text="Checking generated emails...") if st else None

        for i, (person, email) in enumerate(candidates):
            if progress:
                progress.progress(
                    (i + 1) / len(candidates),
                    text=f"Checking {email}...",
                )

            result = self.check_email(email, use_api_if_available=use_api_if_available)

            results[email] = {
                "person":  person,
                "status":  result.get("status", "error"),
                "count":   result.get("count", 0),
                "details": result.get("details", []),
                "method":  result.get("method", "unknown"),
            }

            if st:
                if result.get("status") == "leaked":
                    st.warning(
                        f"⚠️ **{email}** ({person}) — found in HIBP!"
                    )
                elif result.get("status") == "safe":
                    st.caption(f"✅ {email} — clean")
                else:
                    st.caption(f"❓ {email} — {result.get('status')}")

            time.sleep(self.sleep_interval)

        if progress:
            progress.empty()

        breached = sum(1 for v in results.values() if v["status"] == "leaked")
        if st:
            if breached:
                st.error(f"🔴 {breached} generated email(s) found in known breaches!")
            else:
                st.success("✅ No breaches found among generated email addresses.")

        return results