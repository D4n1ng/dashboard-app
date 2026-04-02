from ddgs import DDGS
import requests
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import os
import sqlite3
from datetime import datetime
import re
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
import time

# Module Import
from module_people import PeopleScanner
from module_infra import InfraScanner, CompanyEnricher
from module_breach import BreachChecker
from module_code import CodeScanner
from module_social import SocialScanner
from risk_calculator import calculate_organization_risk

# Helper function and filtering for the new combine_results
_JUNK_URL_PATTERNS = re.compile(
    r"activity-\d+|/employees|/impressum|/neuigkeiten|/management|"
    r"/pressemitteilung|/trusteq-gmbh|/jobs|/about|/posts|"
    r"linkedin\.com/company/|trk=|feed/update",
    re.IGNORECASE,
)
_PROFILE_URL_PATTERNS = re.compile(
    r"linkedin\.com/in/[^/]+/?$|"
    r"xing\.com/profile/[^/]+/?$|"
    r"github\.com/[^/]+/?$|"
    r"twitter\.com/[^/]+/?$|"
    r"x\.com/[^/]+/?$",
    re.IGNORECASE,
)
def _normalize_name(name: str) -> str:
    if not name or pd.isna(name):
        return ""
    name = str(name).lower()
    # Remove titles / salutations
    name = re.sub(r"\b(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|ing\.?)\s*", "", name)
    # Remove everything after a dash, pipe, or @ that looks like a job title
    name = re.split(r"\s*[-–|@]\s*", name)[0]
    # Strip punctuation and normalise whitespace
    name = re.sub(r"[^a-zäöüßáéíóúàèìòùâêîôûãñ\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name
def _is_profile_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if _JUNK_URL_PATTERNS.search(url):
        return False
    return bool(_PROFILE_URL_PATTERNS.search(url))
def _flatten_links(raw_links) -> List[str]:
    flat = []
    if not raw_links:
        return flat
    items = raw_links if isinstance(raw_links, list) else [raw_links]
    for item in items:
        if isinstance(item, list):
            for sub in item:
                s = str(sub).strip().replace(" ", "").replace("\n", "")
                if s and s not in flat:
                    flat.append(s)
        elif item:
            s = str(item).strip().replace(" ", "").replace("\n", "")
            if s and s not in flat:
                flat.append(s)
    return flat
def _merge_person_dicts(base: dict, extra: dict) -> dict:
    # Merge Found_Links
    base_links = _flatten_links(base.get("Found_Links", []))
    extra_links = _flatten_links(extra.get("Found_Links", []))
    merged_links = base_links[:]
    for lnk in extra_links:
        if lnk not in merged_links:
            merged_links.append(lnk)
    base["Found_Links"] = merged_links

    # Merge Emails
    base_emails = list(base.get("Emails") or [])
    extra_emails = list(extra.get("Emails") or [])
    base["Emails"] = list(set(base_emails + extra_emails))

    # Fill in any empty scalar fields from extra
    for field in ("Status", "Details", "URL", "Real_Name", "Official_Company"):
        if not base.get(field) and extra.get(field):
            base[field] = extra[field]

    # Keep the most informative Username (prefer ones that look like real handles)
    base_user = str(base.get("Username") or "")
    extra_user = str(extra.get("Username") or "")
    if (
        (not base_user or base_user in ("N/A", "Unknown", ""))
        and extra_user not in ("N/A", "Unknown", "")
    ):
        base["Username"] = extra_user

    return base

# Start of renderinga and code functions in classes
st.set_page_config(page_title="TRUSTEQ SE-Platform", page_icon="🛡️", layout="wide")
class AsyncRateLimiter:
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
    
    async def __aenter__(self):
        now = time.time()
        self.calls = [c for c in self.calls if c > now - self.period]
        if len(self.calls) >= self.max_calls:
            sleep_time = self.calls[0] + self.period - now
            await asyncio.sleep(max(0, sleep_time))
        self.calls.append(now)
        return self
    
    async def __aexit__(self, *args):
        pass

class CacheManager:
    def __init__(self, db_name="scan_cache.db"):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS cache
                            (cache_key TEXT PRIMARY KEY,
                             timestamp TEXT,
                             data TEXT)''')

    def save(self, key, data):
        # Convert DataFrames to dicts for storage
        serializable_data = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'people': data['people'].to_dict(orient='records') if not data['people'].empty else [],
            'infra': data['infra'],
            'code': data['code'].to_dict(orient='records') if not data['code'].empty else [],
            'subdomains': data['subdomains'],
            'enrichment': data['enrichment']
        }
        json_data = json.dumps(serializable_data)
        
        with sqlite3.connect(self.db_name) as conn:
            conn.execute('''INSERT OR REPLACE INTO cache (cache_key, timestamp, data) 
                            VALUES (?, ?, ?)''', 
                         (key, serializable_data['timestamp'], json_data))

    def load(self, key):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.execute('SELECT data FROM cache WHERE cache_key = ?', (key,))
            row = cursor.fetchone()
            
        if row:
            entry = json.loads(row[0])
            return {
                'people': pd.DataFrame(entry['people']) if entry['people'] else pd.DataFrame(),
                'infra': entry['infra'],
                'code': pd.DataFrame(entry['code']) if entry['code'] else pd.DataFrame(),
                'subdomains': entry['subdomains'],
                'enrichment': entry['enrichment'],
                'timestamp': entry['timestamp'],
                'safe_search': entry.get('safe_search', {}),
                'breach_results': entry.get('breach_results', {}),
                'is_cached': True
            }
        return None

    def _load_file(self):
        if not os.path.exists(self.db_name):
            return {}
        try:
            with open(self.db_name, 'r') as f:
                return json.load(f)
        except:
            return {}

class OSINTCollector:
    def __init__(self, target_company, target_domain, github_token=None, hibp_key=None):
        self.target_company = target_company
        self.target_domain = target_domain
        self.github_token = github_token
        
        # Initialize scanners
        self.infra_scanner = InfraScanner(target_domain)
        self.enricher = CompanyEnricher()
        self.social_scanner = SocialScanner()
        self.people_scanner = PeopleScanner(target_company)
        self.breach_checker = BreachChecker(hibp_key) 

        # Optimized CodeScanner settings
        self.code_scanner = CodeScanner(
            target_company=target_company,
            github_token=github_token,
            max_iterations=2,
            verbose=False
        )
        
        self.cache_manager = CacheManager()
        self.cache_key = f"{target_company}_{target_domain}"
        
        # Performance tracking
        self.execution_times = {}


    def _enrich_and_pivot(self, found_entities, df_github_users):
        st.info("🔄 Phase 5: Enriching identities and pivoting across sources...")

        # 1. Collect all candidates from both OSINT and GitHub
        all_candidates = {}  # key → entity dict (deduplicated by name/username)

        for entity in found_entities:
            key = (entity.get("username") or entity.get("name", "")).lower()
            if key and key != "unknown":
                all_candidates[key] = entity

        if not df_github_users.empty:
            for _, user in df_github_users.iterrows():
                username = str(user.get("login") or user.get("Username") or "")
                real_name = str(user.get("name") or user.get("Real_Name") or username)
                key = username.lower() or real_name.lower()
                if key and key not in all_candidates:
                    all_candidates[key] = {
                        "name": real_name,
                        "username": username,
                        "url": f"https://github.com/{username}" if username else None,
                        "source": "GitHub",
                        "emails": user.get("Emails", []),
                        "scraped_social_links": [],
                        "snippet": "",
                    }

        if not all_candidates:
            st.info("ℹ️ No candidates to enrich.")
            return found_entities

        st.write(f"🔍 Enriching {len(all_candidates)} unique identities...")

        # 2. Run pivot + social searches in parallel
        def _pivot_task(entity):
            results = {
                "base_data": entity,
                "extra_links": list(entity.get("scraped_social_links") or []),
            }
            name = entity.get("name", "")
            username = entity.get("username", "")

            # DuckDuckGo pivot on name + company
            if name and name.lower() not in ("unknown", ""):
                hits = self.people_scanner.search_duckduckgo(
                    query=f'"{name}" "{self.target_company}"', limit=2
                )
                for h in hits:
                    link = h.get("Link") or h.get("href")
                    if link and link not in results["extra_links"]:
                        results["extra_links"].append(link)

                # Social scan on real name
                social_hits = self.social_scanner.search_entity_globally(
                    name, self.target_company
                )
                for hit in social_hits:
                    link = hit.get("Found_URL") or hit.get("Link") or hit.get("link")
                    if link and link not in results["extra_links"]:
                        results["extra_links"].append(link)

            # DuckDuckGo pivot on username
            if username and username.lower() not in ("unknown", "n/a", ""):
                hits = self.people_scanner.search_duckduckgo(
                    query=f'"{username}" "{self.target_company}"', limit=2
                )
                for h in hits:
                    link = h.get("Link") or h.get("href")
                    if link and link not in results["extra_links"]:
                        results["extra_links"].append(link)

            return results

        enriched = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(_pivot_task, entity): entity
                for entity in all_candidates.values()
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    # Merge extra_links back into the entity
                    entity = result["base_data"]
                    entity["scraped_social_links"] = result["extra_links"]
                    enriched.append(entity)
                except Exception as e:
                    print(f"⚠️ Enrich task failed: {e}")

        st.write(f"✅ Enrichment done: {len(enriched)} profiles processed")
        return enriched

    def _combine_people_results(self, df_osint: pd.DataFrame, df_github: pd.DataFrame) -> pd.DataFrame:
        # 1. Collect all rows into dicts
        candidates: List[dict] = []

        def _row_to_dict(row, source_label: str) -> dict:
            links = []
            for col in ("URL", "url", "profile_url", "Link"):
                val = row.get(col)
                if val and pd.notna(val) and str(val).strip() not in links:
                    links.append(str(val).strip())

            if "Found_Links" in row:
                links = _flatten_links(row.get("Found_Links")) or links

            name = str(row.get("Name") or row.get("Real_Name") or "Unknown").strip()
            username = str(row.get("Username") or "N/A").strip()

            # Discard username if it's clearly a URL slug / path, not a handle
            if re.search(r"[/\s]|activity-\d+", username):
                username = "N/A"

            return {
                "Name": name,
                "Username": username,
                "Source": str(row.get("Source") or source_label),
                "Status": str(row.get("Status") or source_label),
                "Official_Company": str(row.get("Official_Company") or row.get("Company") or self.target_company),
                "Found_Links": links,
                "Emails": list(row.get("Emails") or []),
                "URL": row.get("URL") or row.get("url") or (links[0] if links else None),
                "Details": str(row.get("Details") or ""),
                "Real_Name": str(row.get("Real_Name") or name),
                "_norm": _normalize_name(name),
            }

        if not df_osint.empty:
            for _, row in df_osint.iterrows():
                candidates.append(_row_to_dict(row, "OSINT"))

        if not df_github.empty:
            for _, row in df_github.iterrows():
                candidates.append(_row_to_dict(row, "GitHub"))

        # 2. Deduplicate by normalised name 
        merged: List[dict] = []

        def _names_overlap(norm_a: str, norm_b: str) -> bool:
            tokens_a = set(norm_a.split())
            tokens_b = set(norm_b.split())
            if len(tokens_a) < 2 or len(tokens_b) < 2:
                return False
            common = tokens_a & tokens_b
            return len(common) >= 2

        for candidate in candidates:
            norm = candidate["_norm"]
            if not norm or norm == "unknown":
                merged.append(candidate)
                continue

            # Try to find an existing merged entry to absorb into
            matched = None
            for existing in merged:
                existing_norm = existing.get("_norm", "")
                if not existing_norm:
                    continue
                # Exact match
                if norm == existing_norm:
                    matched = existing
                    break
                # Partial token overlap
                if _names_overlap(norm, existing_norm):
                    matched = existing
                    break

            if matched:
                _merge_person_dicts(matched, candidate)
            else:
                merged.append(candidate)

        # 3. Post-process each merged person 
        for person in merged:
            # Remove the internal normalisation key
            person.pop("_norm", None)

            # Filter junk links, keep only real profile URLs
            all_links = _flatten_links(person.get("Found_Links", []))
            profile_links = [l for l in all_links if _is_profile_url(l)]
            # Fall back to all links if filtering removed everything
            person["Found_Links"] = profile_links if profile_links else all_links

            # Ensure URL points to the best available profile link
            if not person.get("URL") or not _is_profile_url(str(person.get("URL", ""))):
                if person["Found_Links"]:
                    # Prefer LinkedIn > XING > GitHub > anything else
                    for preferred in ("linkedin.com/in/", "xing.com/profile/", "github.com/"):
                        for lnk in person["Found_Links"]:
                            if preferred in lnk.lower():
                                person["URL"] = lnk
                                break
                        if person.get("URL"):
                            break
                    if not person.get("URL"):
                        person["URL"] = person["Found_Links"][0]

        return pd.DataFrame(merged) if merged else pd.DataFrame()


    def _check_github_api_details(self):
        try:
            headers = {}
            if self.github_token:
                headers = {'Authorization': f'token {self.github_token}'}
            
            response = requests.get(
                "https://api.github.com/rate_limit", 
                headers=headers,
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                remaining = data['rate']['remaining']
                limit = data['rate']['limit']
                reset_time = datetime.fromtimestamp(data['rate']['reset'])
                
                return True, remaining, limit, reset_time
            return False, 0, 0, None
        except:
            return False, 0, 0, None

    def check_employee_breaches(self, df_people):
        if df_people.empty or 'Emails' not in df_people.columns:
            return {}
        
        all_emails = []
        email_to_person = {}
        
        for _, person in df_people.iterrows():
            name = person.get('Name', 'Unknown')
            emails = person.get('Emails', [])
            
            if emails and isinstance(emails, list):
                for email in emails:
                    if email and '@' in email and isinstance(email, str):
                        all_emails.append(email)
                        email_to_person[email] = name
            elif emails and isinstance(emails, str) and '@' in emails:
                all_emails.append(emails)
                email_to_person[emails] = name
        
        if not all_emails:
            st.info("ℹ️ No emails found to check for breaches")
            return {}
        
        all_emails = list(set(all_emails))
        st.info(f"🔍 Checking {len(all_emails)} unique emails for breaches...")
        
        breach_results = {}
        progress_bar = st.progress(0, text="Checking emails for breaches...")
        
        for i, email in enumerate(all_emails[:10]):
            progress_bar.progress((i + 1) / min(len(all_emails), 10), 
                                text=f"Checking {email}...")
            
            result = self.breach_checker.check_email(email, use_api_if_available=False)
            
            if result['status'] == 'leaked':
                breach_results[email] = {
                    'person': email_to_person[email],
                    'breaches': result.get('details', []),
                    'count': result.get('count', 0),
                    'method': result.get('method', 'web')
                }
                st.warning(f"⚠️ {email} found in {result.get('count', 0)} breach(es)!")
            elif result['status'] == 'safe':
                st.success(f"✅ {email} is clean (no breaches found)")
            elif result['status'] == 'error':
                st.error(f"❌ Error checking {email}: {result.get('message', result.get('details', 'Unknown error'))}")
            
            time.sleep(1.5)
        
        progress_bar.empty()
        
        if breach_results:
            st.success(f"📊 Found {len(breach_results)} breached email(s)")
        else:
            st.success("✅ No breached emails found!")
        
        return breach_results

    def run_code_scan(self, tech_keywords, people_keywords, found_entities):
        start = time.time()
        
        all_keywords = tech_keywords.union(people_keywords)
        
        prioritized_terms = [self.target_company.lower()]
        prioritized_terms.extend([k for k in all_keywords if len(k) > 3][:5])
        
        self.code_scanner.searched_terms.add(self.target_company.lower())
        self.code_scanner.searched_terms.update(prioritized_terms)
        
        github_result = self.code_scanner.iterative_search(
            external_entities=found_entities[:3]
        )
        
        self.execution_times['code_scan'] = time.time() - start
        return github_result

    async def process_github_users_async(self, df_github_users, limit=3):
        start = time.time()
        
        if df_github_users.empty:
            return []
        
        all_findings = []
        rate_limiter = AsyncRateLimiter(max_calls=2, period=1.0)
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for idx, (_, user) in enumerate(df_github_users.head(limit).iterrows()):
                task = self._process_single_user_async(session, user, rate_limiter)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            for result in results:
                if result:
                    all_findings.extend(result)
        
        self.execution_times['user_processing'] = time.time() - start
        return all_findings

    async def _process_single_user_async(self, session, user, rate_limiter):
        findings = []
        
        username = user.get('Username')
        real_name = user.get('Real_Name') or username
        company = user.get('Company_Field', 'Nicht angegeben')
        is_employee = user.get('Is_Verified_Employee', False)
        
        links = []
        
        github_url = f"https://github.com/{username}" if username else None
        if github_url:
            links.append(github_url)
        
        if user.get('Links'):
            link_dict = user.get('Links', {})
            for key, value in link_dict.items():
                if value and isinstance(value, str) and value not in links:
                    if value != github_url:
                        links.append(value)
        
        if user.get('Links') and 'Social_from_README' in user['Links']:
            readme_links = user['Links']['Social_from_README']
            if isinstance(readme_links, list):
                for link in readme_links:
                    if link and link not in links:
                        links.append(link)
        
        insights = user.get('Profile_Insights', [])
        emails = user.get('Emails', [])
        
        if not real_name:
            return findings
        
        social_hits = []
        if real_name != username:
            async with rate_limiter:
                social_results = await self._search_social_async(session, real_name)
                if social_results:
                    social_hits.extend(social_results)
        
        for hit in social_hits:
            if isinstance(hit, dict):
                link = hit.get('Link') or hit.get('link') or hit.get('url')
                title = hit.get('Title', '').lower()
                snippet = hit.get('Snippet', '').lower()
                
                if link and link not in links:
                    name_parts = real_name.lower().split()
                    is_relevant = False
                    
                    for part in name_parts:
                        if len(part) > 2 and (part in title or part in snippet):
                            is_relevant = True
                            break
                    
                    if 'linkedin.com' in link.lower():
                        is_relevant = True
                    
                    if is_relevant:
                        links.append(link)
                        st.caption(f"      🔗 Added social link: {link[:50]}...")
        
        details_parts = []
        if insights:
            for insight in insights[:2]:
                clean_insight = re.sub(r'\s+', ' ', str(insight))
                clean_insight = clean_insight.replace(' @ ', '@').replace(' . ', '.')
                details_parts.append(clean_insight)
        if emails:
            details_parts.append(f"{len(emails)} email(s) found")

        for hit in social_hits[:1]:
            if isinstance(hit, dict) and hit.get('Snippet'):
                snippet = hit['Snippet'][:150]
                snippet = re.sub(r'\s+', ' ', snippet)
                snippet = snippet.replace(' @ ', '@').replace(' . ', '.')
                snippet = snippet.replace(' [dot] ', '.').replace(' (dot) ', '.')
                snippet = snippet.replace(' [at] ', '@').replace(' (at) ', '@')
                if not any(part in snippet.lower() for part in ['cookie', 'privacy', 'policy']):
                    details_parts.append(snippet)
                    break

        details_text = " • ".join(details_parts) if details_parts else "GitHub user profile"
        
        print(f"👤 Processing {real_name}: Found {len(links)} links, {len(emails)} emails")
        
        findings.append({
            "Name": real_name,
            "Username": username or "N/A",
            "Source": "GitHub Profile",
            "Status": "✅ Verified Employee" if is_employee else "👤 GitHub User",
            "Official_Company": company,
            "Found_Links": links,
            "Emails": emails,
            "URL": github_url,
            "Details": details_text,
            "Real_Name": real_name
        })
        
        return findings

    async def _search_social_async(self, session, name):
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                result = await loop.run_in_executor(
                    executor, 
                    self.social_scanner.search_entity_globally,
                    name, 
                    self.target_company
                )
            
            if result and isinstance(result, list):
                flat_result = []
                for item in result:
                    if isinstance(item, dict):
                        if 'Link' not in item and 'link' in item:
                            item['Link'] = item['link']
                        flat_result.append(item)
                    elif isinstance(item, list):
                        for sub_item in item:
                            if isinstance(sub_item, dict):
                                if 'Link' not in sub_item and 'link' in sub_item:
                                    sub_item['Link'] = sub_item['link']
                                flat_result.append(sub_item)
                            elif isinstance(sub_item, str):
                                flat_result.append({'Link': sub_item, 'Snippet': ''})
                    elif isinstance(item, str):
                        flat_result.append({'Link': item, 'Snippet': ''})
                return flat_result
            return []
        except Exception as e:
            print(f"Error in social search: {e}")
            return []

    def run_full_scan(self):
        # Initialize URL tracking for this scan
        if 'scraped_urls_global' not in st.session_state:
            st.session_state.scraped_urls_global = set()
        
        # Clear for new scan
        st.session_state.scraped_urls_global = set()

        # Pass this set to all relevant scanners (adapt for future modules as needed)
        self.people_scanner.scraped_urls_global = st.session_state.scraped_urls_global
        self.code_scanner.scraped_urls_global = st.session_state.scraped_urls_global

        # Store scan state in session state for cross-page visibility
        st.session_state['is_scanning'] = True
        st.session_state['scan_status'] = "Initialisiere Scan..."
        st.session_state['scan_progress'] = 0
        
        # Setup progress tracking for current page
        progress_bar = st.progress(0, text="Initialisiere Scan...")
        status_text = st.empty()
        
        # Check GitHub API availability and set budget
        api_available, remaining_calls, limit, reset_time = self._check_github_api_details()
        
        api_budget = 0
        if api_available:
            api_budget = min(250, remaining_calls)
            st.info(f"📊 API Budget: {api_budget} calls (of {remaining_calls} remaining)")
            if remaining_calls < 100:
                st.warning(f"⚠️ Only {remaining_calls} API calls remaining. Reset at {reset_time.strftime('%H:%M:%S')}")
        else:
            st.warning("🌐 GitHub API not available - use web search only")

        # Phase 1: Infrastructure 
        status_text.text("🌐 Phase 1/5: Analyzing DNS Records and Web-Header...")
        st.session_state.update(
            scan_status="🌐 Phase 1/5: Analyzing DNS Records and Web-Header...",
            scan_progress=10,
        )
        progress_bar.progress(10)
        
        infra_all = self.infra_scanner.analyze_all()
        dns_data = infra_all['dns']
        web_data = infra_all['web']
        subdomains = infra_all['subdomains']
        safe_search = infra_all['safe_search']
        enrichment = self.enricher.get_details(self.target_domain)
        infra_combined = dns_data + web_data

        #Freely adaptable for more details
        tech_keywords = set()
        for item in infra_combined:
            if 'Software' in item:
                tech_keywords.add(item['Software'].lower())
            if 'Server' in item:
                tech_keywords.add(item['Server'].lower())
        
        if tech_keywords:
            st.write(f"🔧 Found Technologies: {', '.join(list(tech_keywords)[:5])}")

        # Phase 2: OSINT People
        status_text.text("👥 Phase 2/5: Searching Employees via OSINT...")
        st.session_state.update(
            scan_status="👥 Phase 2/5: Searching Employees via OSINT...",
            scan_progress=25,
        )
        progress_bar.progress(25)

        df_people_osint = self.people_scanner.scan_all_sources(limit=10)
        found_entities_from_osint = []
        people_keywords = set()

        if not df_people_osint.empty:
            for _, person in df_people_osint.iterrows():
                raw_name = str(person.get('Scraped_Name') or person.get('Name') or "Unknown")
                
                name_lower = raw_name.lower()
                company_lower = self.target_company.lower()
                blacklist = ["gmbh", "ag", "corp", "inc", "limited", "admin", "support", "team", "info"]
                blacklist_pattern = re.compile(r'\b(' + '|'.join(re.escape(b) for b in blacklist) + r')\b')

                if name_lower == company_lower or company_lower in name_lower or blacklist_pattern.search(name_lower):
                    continue
                
                if len(raw_name.strip()) < 4:
                    continue

                entity = {'name': raw_name}
                
                if 'Username' in person and person['Username']:
                    entity['username'] = str(person['Username'])
                    people_keywords.add(str(person['Username']).lower())
                
                name_parts = raw_name.lower().split()
                people_keywords.update(name_parts)
                
                primary_url = str(person.get('URL') or person.get('Link') or "")
                entity['url'] = primary_url
                entity['scraped_social_links'] = person.get('Scraped_Social_Links', []) or []
                if primary_url:
                    entity['scraped_social_links'].append(primary_url)
                
                entity['emails'] = person.get('Emails', [])
                entity['source'] = str(person.get('Engine') or person.get('Source') or "OSINT")
                entity['snippet'] = str(person.get('Snippet', ''))[:200]
                
                name = entity['name']
                print(f"🔍 Deep Scanning: {name}")
                
                social_hits = self.social_scanner.search_entity_globally(name, self.target_company)
                time.sleep(1.5)

                has_linkedin = any("linkedin.com/in/" in str(l).lower() for l in entity['scraped_social_links'])
                if not has_linkedin:
                    hunt_query = f'site:linkedin.com/in/ "{name}" "{self.target_company}"'
                    try:
                        with DDGS() as ddgs:
                            hunt_res = list(ddgs.text(hunt_query, max_results=2))
                            for hr in hunt_res:
                                social_hits.append({
                                    'Found_URL': hr.get('href'),
                                    'Snippet': hr.get('body'),
                                    'Source': 'Targeted LinkedIn Hunt'
                                })
                    except Exception as e:
                        print(f"⚠️ LinkedIn hunt failed for {name}: {e}")

                verified_social_links = []
                for hit in social_hits:
                    link_url = hit.get('Found_URL', '')
                    hit_context = (hit.get('Snippet', '') + " " + link_url).lower()
                    
                    name_parts = re.split(r'[\s\-]+', name.lower())
                    matched = sum(1 for p in name_parts if p in hit_context)
                    name_match = matched >= max(1, len(name_parts) - 1)
                    is_directory = any(x in link_url.lower() for x in ["/orgs/", "/followers", "/stargazers", "/trending"])
                    
                    if name_match and not is_directory:
                        verified_social_links.append(link_url)

                entity['scraped_social_links'] = list(set(entity['scraped_social_links'] + verified_social_links))
                found_entities_from_osint.append(entity)
        # Merge people discovered during scraping
        if hasattr(self.people_scanner, "discovered_people"):
            existing_names_low = {
                e.get("name", "").lower() for e in found_entities_from_osint
            }
            for person in self.people_scanner.discovered_people:
                name_lower = person["name"].lower()
                if name_lower in existing_names_low:
                    # Merge URL into existing entity
                    for entity in found_entities_from_osint:
                        if entity.get("name", "").lower() == name_lower:
                            new_url = person.get("url")
                            if new_url:
                                existing_url = entity.get("url")
                                if isinstance(existing_url, list):
                                    if new_url not in existing_url:
                                        existing_url.append(new_url)
                                elif existing_url and existing_url != new_url:
                                    entity["url"] = [existing_url, new_url]
                                else:
                                    entity["url"] = new_url
                            break
                else:
                    found_entities_from_osint.append(
                        {
                            "name": person["name"],
                            "url": person.get("url"),
                            "source": person.get("source", "Discovered from page"),
                            "snippet": f"Discovered from {person.get('source', 'unknown page')}",
                        }
                    )
                    existing_names_low.add(name_lower)
                    st.caption(f"  ✅ Added discovered person: {person['name']}")

        st.write(f"👤 Found Entities from OSINT: {len(found_entities_from_osint)}")
        if found_entities_from_osint:
            with_urls = sum(1 for e in found_entities_from_osint if e.get('url'))
            st.caption(f"   📎 Davon mit URLs: {with_urls}")

        # Phase 3: Social Media
        status_text.text("🔄 Phase 3/5: Searching for Social Media Profiles...")
        st.session_state.update(
            scan_status="🔄 Phase 3/5: Searching for Social Media Profiles...",
            scan_progress=40,
        )
        progress_bar.progress(40)

        social_entities = []
        if not df_people_osint.empty:
            for _, person in df_people_osint.head(3).iterrows():
                name = person.get('Name', '')
                if name:
                    social_hits = self.social_scanner.search_entity_globally(name, self.target_company)
                    social_profile_urls = self.social_scanner.search_social_profiles(
                        name, self.target_company
                    )
                    for url in social_profile_urls:
                        if url not in entity['scraped_social_links']:
                            entity['scraped_social_links'].append(url)
                            print(f"  📱 Added social profile: {url}")
                    for hit in social_hits:
                        social_entity = {'name': name}
                        
                        if 'Link' in hit and hit['Link']:
                            social_entity['url'] = hit['Link']
                        
                        if 'Link' in hit and hit['Link']:
                            username_match = re.search(r'/([^/]+)/?$', hit['Link'])
                            if username_match:
                                potential_username = username_match.group(1)
                                if potential_username and len(potential_username) > 2:
                                    social_entity['username'] = potential_username
                        
                        if 'Snippet' in hit and hit['Snippet']:
                            social_entity['snippet'] = hit['Snippet'][:200]
                            words = str(hit['Snippet']).lower().split()
                            people_keywords.update([w for w in words if len(w) > 3])
                        
                        if 'Platform' in hit:
                            social_entity['source'] = f"Social Media ({hit['Platform']})"
                        else:
                            social_entity['source'] = "Social Media Search"
                        
                        if social_entity.get('url'):
                            social_entities.append(social_entity)

        found_entities_from_osint.extend(social_entities)

        # Phase 4: GitHub Scan
        github_result = None
        df_code = pd.DataFrame()
        df_github_users = pd.DataFrame()
        is_cached = False
        used_web_fallback = False
        
        self.code_scanner.max_api_calls = api_budget
        self.code_scanner.api_calls_used = 0
        
        if api_available and api_budget > 20:
            status_text.text(f"🔍 Phase 4/5: GitHub API Scan (Budget: {api_budget} Calls)...")
            st.session_state.update(
                scan_status=f"🔍 Phase 4/5: GitHub API Scan (Budget: {api_budget} Calls)...",
                scan_progress=60,
            )
            
            self.code_scanner.use_web_always = False
            self.code_scanner.using_fallback = False
        else:
            status_text.text("🔍 Phase 4/5: GitHub Web-Fallback Scan...")
            st.session_state.update(
                scan_status="🔍 Phase 4/5: GitHub Web-Fallback Scan...",
                scan_progress=60,
            )
            
            self.code_scanner.use_web_always = True
            self.code_scanner.using_fallback = True
            used_web_fallback = True

        progress_bar.progress(60)
        self.code_scanner.searched_terms.add(self.target_company.lower())
        priority_terms = [k for k in people_keywords if len(k) > 3][:5]
        self.code_scanner.searched_terms.update(priority_terms)
        
        st.info("⏱️ GitHub Scan runs (max 2-3 minutes)...")
        
        try:
            github_result = self.run_code_scan(tech_keywords, people_keywords, found_entities_from_osint)
            
            if api_available and not used_web_fallback:
                st.caption(f"📊 Verbrauchte API Calls: {self.code_scanner.api_calls_used}/{api_budget}")
                if self.code_scanner.api_calls_used >= api_budget * 0.8:
                    st.warning("⚠️ API Budget fast aufgebraucht")
            
            if github_result and not (github_result.repos.empty and github_result.users.empty):
                df_code = github_result.repos
                df_github_users = github_result.users
                is_cached = False
                
                if used_web_fallback:
                    st.success(f"✅ Web-Fallback Scan successful: {len(df_code)} Repos, {len(df_github_users)} Users")
                else:
                    st.success(f"✅ API Scan successful: {len(df_code)} Repos, {len(df_github_users)} Users")
            else:
                st.warning("⚠️ GitHub Scan showed no results. Trying cache...")
                cached_data = self.cache_manager.load(self.cache_key)
                if cached_data:
                    st.info("📦 Using cached GitHub-Data...")
                    df_code = cached_data['code']
                    is_cached = True
                else:
                    st.warning("ℹ️ No cached GitHub-Data available.")
                    
        except Exception as e:
            st.error(f"❌ GitHub Scan failed: {e}")
            cached_data = self.cache_manager.load(self.cache_key)
            if cached_data:
                st.info("📦 Using cached GitHub-Data...")
                df_code = cached_data['code']
                is_cached = True

        progress_bar.progress(80)

        # Phase 5: Pivot & Combine 
        status_text.text("🕵️ Phase 5/5: Analyze and combine findings...")
        st.session_state.update(
                scan_status="🕵️ Phase 5/5: Analyze and combine findings...",
                scan_progress=80,
            )
        progress_bar.progress(80)

        enriched_entities = self._enrich_and_pivot(found_entities_from_osint, df_github_users)
        
        df_github_pivoted = pd.DataFrame()

        if not df_github_users.empty:
            login_col = next(
                (c for c in df_github_users.columns if c.lower() in ['login', 'username', 'user', 'name', 'github_user']),
                df_github_users.columns[0]
            )
            cache_key = "github_scan_" + str(sorted(df_github_users[login_col].tolist()))          
            if cache_key not in st.session_state:
                st.write(f"Analyze {min(5, len(df_github_users))} GitHub-Entities from Social Media...")
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    all_findings = loop.run_until_complete(
                        self.process_github_users_async(df_github_users, limit=5)
                    )
                    st.session_state[cache_key] = all_findings
                finally:
                    loop.close()
            
            cached = st.session_state.get(cache_key, [])
            if cached:
                df_github_pivoted = pd.DataFrame(cached)
        # Build final people DataFrame using the fixed _combine_people_results
        # Convert enriched_entities list back to a DataFrame for the combiner
        df_enriched_osint = pd.DataFrame(
            [
                {
                    "Name": e.get("name", "Unknown"),
                    "Username": e.get("username", "N/A"),
                    "Source": e.get("source", "OSINT"),
                    "Status": "OSINT Found",
                    "Company": self.target_company,
                    "Found_Links": e.get("scraped_social_links", []),
                    "Emails": e.get("emails", []),
                    "URL": e.get("url"),
                    "Details": e.get("snippet", "")[:200],
                    "Real_Name": e.get("name", "Unknown"),
                }
                for e in enriched_entities
            ]
        )

        df_p_final = self._combine_people_results(df_enriched_osint, df_github_pivoted)

        progress_bar.progress(100)
        status_text.text("✅ Scan finished!")
        st.session_state.update(
            scan_status="✅ Scan finished!",
            scan_progress=100,
            is_scanning=False,
        )

        if df_p_final.empty and df_code.empty:
            st.warning("⚠️ No data found.")
        else:
            total_links = (
                df_p_final["Found_Links"].apply(len).sum()
                if not df_p_final.empty and "Found_Links" in df_p_final
                else 0
            )
            total_emails = (
                df_p_final["Emails"].apply(len).sum()
                if not df_p_final.empty and "Emails" in df_p_final
                else 0
            )
            st.success(
                f"✅ Scan finished! Found: {len(df_p_final)} Employees, "
                f"{len(df_code)} Repositories, {total_links} Links, {total_emails} Emails"
            )

        if is_cached:
            st.info("📦 Attention: Much of the data is from the Cache")

        if not is_cached and not df_code.empty:
            self.cache_manager.save(
                self.cache_key,
                {
                    "people": df_p_final,
                    "infra": infra_combined,
                    "code": df_code,
                    "subdomains": subdomains,
                    "enrichment": enrichment,
                },
            )

        st.markdown("---")
        st.subheader("🔐 Breach Detection")

        breach_results = {}
        generated_breach_results = {}

        with st.expander("Check emails for data breaches", expanded=False):

            st.markdown("**Scraped emails**")
            st.info("Checking discovered emails against Have I Been Pwned...")
            breach_results = self.check_employee_breaches(df_p_final)
            if breach_results:
                st.session_state['breach_results'] = breach_results

            st.markdown("---")
            st.markdown("**Generated corporate emails** (`firstname.lastname@domain`)")

            generated_breach_results = self.breach_checker.generate_and_check_emails(
                df_people=df_p_final,
                domain=self.target_domain,
                use_api_if_available=bool(self.breach_checker.api_key),
                streamlit_ui=st,
            )
            if generated_breach_results:
                st.session_state['generated_breach_results'] = generated_breach_results

        return (
            df_p_final, infra_combined, df_code,
            subdomains, enrichment, is_cached,
            safe_search, breach_results, generated_breach_results,
        )


    def _is_valid_url(self, url):
        if not url or not isinstance(url, str):
            return False
        
        url_patterns = [
            r'^https?://',
            r'^www\.',
            r'\.com/',
            r'\.de/',
            r'\.org/',
            r'linkedin\.com',
            r'twitter\.com',
            r'x\.com',
            r'github\.com',
            r'facebook\.com',
            r'instagram\.com'
        ]
        
        url = str(url).lower()
        return any(re.search(pattern, url) for pattern in url_patterns)


def main():
    st.sidebar.title("🛡️ TRUSTEQ OSINT")
    target_company = st.sidebar.text_input("Company Name", value="trusteq")
    target_domain = st.sidebar.text_input("Domain", value="trusteq.de")
    
    with st.sidebar.expander("🔑 API Keys"):
        github_token = st.text_input("GitHub Token", type="password", 
                                    help="From https://github.com/settings/tokens (repo + user scopes)")
        hibp_key = st.text_input("HIBP API Key (optional)", type="password",
                                help="Leave empty to use web method")
        
        if github_token:
            test_headers = {'Authorization': f'token {github_token}'}
            try:
                test_res = requests.get("https://api.github.com/rate_limit", headers=test_headers, timeout=3)
                if test_res.status_code == 200:
                    data = test_res.json()
                    st.success(f"✅ Token valid! {data['rate']['remaining']} calls left")
                else:
                    st.error("❌ Token invalid!")
            except:
                st.error("❌ Could not test token")

    st.sidebar.markdown("---")
    page = st.sidebar.radio("Navigation", ["Dashboard Summary", "Found Employees", "Code Leaks", "Breach Results"])

    collector = OSINTCollector(target_company, target_domain, github_token, hibp_key)

    if st.sidebar.button("Start Scan"):
        # Reset scan state
        st.session_state['is_scanning'] = False
        st.session_state['scan_results'] = None
        
        with st.spinner(f"Scan runs (takes 2-4 minutes)..."):
            df_p, infra, df_c, subs, enrich, is_cached, safe_search, breach_results, generated_breach_results = collector.run_full_scan()
            
            st.session_state['scan_results'] = {
                'people':                   df_p if not df_p.empty else pd.DataFrame(),
                'infra':                    infra,
                'code':                     df_c,
                'subdomains':               subs,
                'enrichment':               enrich,
                'timestamp':                datetime.now().strftime('%H:%M:%S'),
                'safe_search':              safe_search,
                'is_cached':                is_cached,
                'breach_results':           breach_results,
                'generated_breach_results': generated_breach_results,
            }
            
            if is_cached:
                st.sidebar.warning("⚠️ Showing cached data")
            else:
                st.sidebar.success("✅ Scan complete!")
            
            st.rerun()

    results = st.session_state.get('scan_results', None)
    
    if page == "Dashboard Summary":
        render_dashboard(results, collector)
    elif page == "Found Employees":
        render_people_page(results)
    elif page == "Code Leaks":
        render_code_page(results)
    elif page == "Breach Results":
        render_breach_page(results)

def render_dashboard(results, collector):
    # Check if a scan is running
    if st.session_state.get('is_scanning', False):
        st.markdown("### 📊 Live Scan Progress")
        st.markdown("---")
        
        progress = st.session_state.get('scan_progress', 0)
        status = st.session_state.get('scan_status', "Initializing...")
        
        st.progress(progress / 100)
        st.info(f"**Current Phase:** {status}")
        st.markdown("---")
        st.info("Scan runs in the background. ")
        
        time.sleep(2)
        st.rerun()
        return
    
    # If not scanning, show the dashboard as before
    if not results:
        st.info("Please start a scan.")
        return

    if results.get('is_cached'):
        st.warning(f"⚠️ **ATTENTION:** API Rate Limit active. Showing data from the last successful scan ({results.get('timestamp')}).")

    st.title(f"🛡️ Surface Overview: {collector.target_company}")
    st.markdown(f"**Domain:** {collector.target_domain} | **Last Scan:** {results.get('timestamp')}")
    st.divider()

    risk_data = calculate_organization_risk(
        infra_data=results['infra'],
        subdomains=results['subdomains'],
        code_df=results['code'],
        people_df=results['people']
    )
    
    avg_risk = risk_data['score']
    risk_label = risk_data['label']
    
    gauge_color = "green"
    if risk_label == "CRITICAL": gauge_color = "darkred"
    elif risk_label == "HIGH": gauge_color = "red"
    elif risk_label == "MEDIUM": gauge_color = "orange"
    
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta", 
            value=avg_risk, 
            title={'text': f"Risk Score: {risk_label}"},
            gauge={'axis': {'range': [0, 100]}, 'bar': {'color': gauge_color},
                   'steps': [
                       {'range': [0, 25], 'color': "lightgreen"}, 
                       {'range': [25, 50], 'color': "lightyellow"},
                       {'range': [50, 75], 'color': "lightsalmon"},
                       {'range': [75, 100], 'color': "salmon"}
                    ]}))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.metric("Employees", len(results['people']) if not results['people'].empty else 0)
        st.metric("Repos", len(results['code']) if not results['code'].empty else 0)
    with c3:
        st.subheader("Risk Breakdown")
        st.caption(f"Infrastructure: {risk_data['breakdown']['Infrastructure']}/100")
        st.caption(f"Code/GitHub: {risk_data['breakdown']['Code']}/100")
        st.caption(f"People OSINT: {risk_data['breakdown']['People']}/100")

    st.divider()
    if results['subdomains']:
        st.subheader("⚠️ Critical Subdomains")
        for sub in results['subdomains']:
            st.error(f"{sub.get('Portal', 'Unknown')}")

    st.divider()
    st.subheader("🔍 Google Safe Browsing")

    safe = results.get('safe_search', {})
    status_text = safe.get('status', 'Not checked')
    report_url = safe.get('url', '')

    if '✅' in status_text:
        st.success(status_text)
    elif '❌' in status_text:
        st.error(status_text)
    elif '⚠️' in status_text:
        st.warning(status_text)
    else:
        st.info(status_text)

    if report_url:
        st.markdown(f"[🔗 View full Google Transparency Report]({report_url})")

def render_people_page(results):
    st.subheader("👥 Discovered Identities")
    
    if st.session_state.get('is_scanning', False):
        st.info("🔄 Scan runs currently... The results will be displayed after completion.")
        return
    
    if not results or results.get('people', pd.DataFrame()).empty:
        st.info("No employee data available. Start a scan to discover identities.")
        return
    
    if results and not results['people'].empty:
        for _, person in results['people'].iterrows():
            username = person.get('Username')
            if pd.isna(username) or username is None:
                username = "unknown"
            else:
                username = str(username)
            
            name = person.get('Name')
            if pd.isna(name) or name is None:
                name = "Unknown"
            else:
                name = str(name)
            
            source = person.get('Source')
            if pd.isna(source) or source is None:
                source = "OSINT"
            else:
                source = str(source)
            
            display_title = f"👤 {name}"
            if username and username != "unknown":
                display_title += f" (@{username})"
            if source:
                display_title += f" - {source}"
            
            with st.expander(display_title):
                st.write(f"**Status:** {person.get('Status', 'Unknown')}")
                st.write(f"**Company:** {person.get('Official_Company', 'Unknown')}")
                
                if 'Details' in person and person['Details'] and pd.notna(person['Details']):
                    details = str(person['Details'])
                    details = re.sub(r'\s+', ' ', details)
                    details = details.replace(' @ ', '@').replace(' . ', '.')
                    st.info(details)
                
                links = person.get('Found_Links', [])
                if links and len(links) > 0:
                    st.write("**🔗 Found Profiles & Mentions:**")
                    flat_links = []
                    for link in links:
                        if isinstance(link, list):
                            flat_links.extend([str(l) for l in link if l and str(l) not in flat_links])
                        elif link and str(link) not in flat_links:
                            flat_links.append(str(link))
                    
                    for link in flat_links:
                        if link and isinstance(link, str):
                            clean_link = link.strip().replace(' ', '').replace('\n', '')
                            icon = "🔗"
                            link_lower = clean_link.lower()
                            if 'linkedin.com' in link_lower:
                                icon = "💼"
                            elif 'twitter.com' in link_lower or 'x.com' in link_lower:
                                icon = "🐦"
                            elif 'github.com' in link_lower:
                                icon = "🐙"
                            st.write(f"- {icon} [{clean_link}]({clean_link})")
                
                profile_url = person.get('URL') or person.get('Profile_URL')
                if profile_url and pd.notna(profile_url):
                    st.markdown(f"--- \n [🔍 View Primary Profile]({str(profile_url)})")
    else:
        st.info("No employee data available. Start a scan to discover identities.")

def render_code_page(results):
    st.subheader("💻 Critical Code Repositories")
    
    if st.session_state.get('is_scanning', False):
        st.info("🔄 Scan runs currently... The results will be displayed after completion.")
        return
    
    if not results or results.get('code', pd.DataFrame()).empty:
        st.info("No repositories available. Start a scan to discover code leaks.")
        return

    if results and not results['code'].empty:
        if results.get('is_cached'):
            st.warning("⚠️ Display based on cache data.")
        
        for _, repo in results['code'].iterrows():
            risk_score = repo.get('risk_score', 0)
            
            with st.container():
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f" 📂 {repo.get('repo_name', 'Unknown')}")
                    st.caption(f"URL: {repo.get('url', 'N/A')}")
                with col2:
                    st.metric("Risk Score", f"{risk_score}/100")
                
                st.progress(min(risk_score / 100, 1.0))
                repo_url = repo.get('url')
                if repo_url:
                    st.markdown(f"[Inspect Repository]({repo_url})")
                st.divider()
    else:
        st.info("No repositories found.")

def render_breach_page(results):
    st.subheader("🔐 Breach Results")

    if st.session_state.get('is_scanning', False):
        st.info("🔄 Scan running... breach results will appear after completion.")
        return

    if not results:
        st.info("No breach data available. Run a scan first.")
        return

    tab_scraped, tab_generated = st.tabs(
        ["📥 Scraped emails", "🔮 Generated (firstname.lastname)"]
    )

    # Scraped emails 
    with tab_scraped:
        breach_results = results.get('breach_results', {})
        if not breach_results:
            st.info("No scraped email breach data. Run a scan with breach checking enabled.")
        else:
            st.warning(f"⚠️ {len(breach_results)} compromised scraped email(s) found!")
            for email, data in breach_results.items():
                with st.expander(f"📧 {email} — {data['person']}"):
                    st.write(f"**Person:** {data['person']}")
                    st.write(f"**Breaches:** {data['count']}")
                    st.write(f"**Method:** {data['method']}")
                    if data.get('breaches'):
                        for breach in data['breaches']:
                            st.write(f"- {breach}")
                    st.markdown(
                        f"[🔍 View on HIBP](https://haveibeenpwned.com/account/{email})"
                    )

    # Generated emails 
    with tab_generated:
        gen_results = results.get('generated_breach_results', {})
        if not gen_results:
            st.info("No generated email results yet. Run a scan to check guessed addresses.")
            return

        leaked = {e: d for e, d in gen_results.items() if d['status'] == 'leaked'}
        safe   = {e: d for e, d in gen_results.items() if d['status'] == 'safe'}
        errors = {e: d for e, d in gen_results.items() if d['status'] == 'error'}

        col1, col2, col3 = st.columns(3)
        col1.metric("🔴 Breached", len(leaked))
        col2.metric("✅ Clean",    len(safe))
        col3.metric("❓ Errors",   len(errors))

        if leaked:
            st.markdown("### 🔴 Breached")
            for email, data in leaked.items():
                with st.expander(f"📧 {email} — {data['person']}"):
                    st.write(f"**Person:** {data['person']}")
                    st.write(f"**Breaches:** {data['count']}")
                    st.write(f"**Method:** {data['method']}")
                    if data.get('details'):
                        st.write("**Found in:**")
                        for breach in data['details']:
                            st.write(f"  - {breach}")
                    st.markdown(
                        f"[🔍 Check on HIBP](https://haveibeenpwned.com/account/{email})"
                    )

        if safe:
            with st.expander(f"✅ Clean ({len(safe)})"):
                for email, data in safe.items():
                    st.caption(f"✅ {email} — {data['person']}")

        if errors:
            with st.expander(f"❓ Errors ({len(errors)})"):
                for email, data in errors.items():
                    st.caption(f"❓ {email} — {data.get('details', 'unknown error')}")

if __name__ == "__main__":
    main()
