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
    # TODO no results from this and no prints
    async def run_enrichment_phase(self, initial_results):
        st.info("🔄 Phase 2: Enriching and pivoting on discovered identities...")
        all_people = []
        
        # 1. Extract from People module
        if 'people' in initial_results and isinstance(initial_results['people'], pd.DataFrame) and not initial_results['people'].empty:
            all_people.extend(initial_results['people'].to_dict('records'))
            
        # 2. Extract from Code module (e.g., GitHub users)
        if 'code' in initial_results and 'users' in initial_results['code']:
            code_users = initial_results['code']['users']
            if isinstance(code_users, pd.DataFrame) and not code_users.empty:
                for _, user in code_users.iterrows():
                    all_people.append({
                        'Name': user.get('name', 'Unknown'),
                        'Username': user.get('login', ''),
                        'Profile_URL': user.get('html_url', ''),
                        'Source': 'Code_Module'
                    })
                
        # 3. Deduplicate entities
        unique_entities = {}
        for person in all_people:
            key = person.get('Username') or person.get('Name')
            if key and key != 'Unknown':
                if key not in unique_entities:
                    unique_entities[key] = person

        # 4. Rerun Pivot and Social Scans in parallel
        enriched_profiles = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._pivot_and_social_task, entity) for entity in unique_entities.values()]
            for future in as_completed(futures):
                enriched_profiles.append(future.result())
                
        initial_results['enriched_identities'] = enriched_profiles
        return initial_results
    # TODO no results from this and no prints
    def _pivot_and_social_task(self, entity):
        results = {'base_data': entity, 'pivot_findings': [], 'social_links': []}
        name = entity.get('Name')
        username = entity.get('Username')
        
        if name and name != "Unknown":
            query_name = f'"{name}" "{self.target_company}"'
            results['pivot_findings'].extend(self.people_scanner.search_duckduckgo(query=query_name, limit=2))
            results['social_links'].extend(self.social_scanner.search_entity_globally(name, self.target_company)) 

        if username and username != "Unknown":
            query_user = f'"{username}" "{self.target_company}"'
            results['pivot_findings'].extend(self.people_scanner.search_duckduckgo(query=query_user, limit=2))

        return results
    
    async def run_parallel_scans(self):
        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.create_task(self._scan_infrastructure()),
                asyncio.create_task(self._scan_people_osint()),
                asyncio.create_task(self.run_code_scan(set(), set(), [])),
                asyncio.create_task(self.process_github_users_async(pd.DataFrame())),
                asyncio.create_task(self.run_full_osint_process()),
            ]
            
            results = await asyncio.gather(*tasks)
            return results
            
    # TODO no results from this and no prints
    async def run_full_osint_process(self):
        st.info("🔍 Phase 1: Running initial module discovery...")
        results = await self.run_parallel_scans() 
        enriched_data = await self.run_enrichment_phase(results)
        
        if self.hibp_api_key:
            st.info("📧 Phase 3: Checking discovered emails for breaches...")
            emails_to_check = self.extract_all_emails(enriched_data)
            breach_results = {}
            for email in emails_to_check:
                breach_results[email] = self.breach_scanner.check_hibp(email, self.hibp_api_key)
                time.sleep(1.6)
            enriched_data['breach_report'] = breach_results

        return enriched_data

    async def _scan_infrastructure(self):
        start = time.time()
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            dns_future = executor.submit(self.infra_scanner.analyze_dns_txt)
            web_future = executor.submit(self.infra_scanner.analyze_web_headers)
            subdomain_future = executor.submit(self.infra_scanner.check_subdomains)
            
            dns_data = dns_future.result()
            web_data = web_future.result()
            subdomains = subdomain_future.result()
        
        enrichment = self.enricher.get_details(self.target_domain)
        infra_combined = dns_data + web_data
        
        self.execution_times['infrastructure'] = time.time() - start
        return infra_combined, subdomains, enrichment

    async def _scan_people_osint(self):
        start = time.time()
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                self.people_scanner.scan_all_sources, 
                limit=5  
            )
            df_people = future.result()
        
        people_keywords = set()
        found_entities = []
        
        if not df_people.empty:
            for _, person in df_people.iterrows():
                entity = {}
                if 'Name' in person and person['Name']:
                    entity['name'] = str(person['Name'])
                    name_parts = str(person['Name']).lower().split()
                    people_keywords.update(name_parts)
                
                if 'Username' in person and person['Username']:
                    entity['username'] = str(person['Username'])
                    people_keywords.add(str(person['Username']).lower())
                
                if entity:
                    found_entities.append(entity)
        
        self.execution_times['people_osint'] = time.time() - start
        return df_people, people_keywords, found_entities

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
            "Offizielle_Firma": company,
            "Gefundene_Links": links,
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
                st.warning(f"⚠️ Nur {remaining_calls} API Calls übrig. Reset um {reset_time.strftime('%H:%M:%S')}")
        else:
            st.warning("🌐 GitHub API nicht verfügbar - verwende nur Web-Suche")

        # Phase 1: Infrastructure 
        status_text.text("🌐 Phase 1/5: Analyzing DNS Records and Web-Header...")
        st.session_state['scan_status'] = "🌐 Phase 1/5: Analyzing DNS Records and Web-Header..."
        st.session_state['scan_progress'] = 10
        progress_bar.progress(10)
        
        infra_all = self.infra_scanner.analyze_all()
        dns_data = infra_all['dns']
        web_data = infra_all['web']
        subdomains = infra_all['subdomains']
        safe_search = infra_all['safe_search']
        enrichment = self.enricher.get_details(self.target_domain)
        infra_combined = dns_data + web_data

        tech_keywords = set()
        for item in infra_combined:
            if 'Software' in item:
                tech_keywords.add(item['Software'].lower())
            if 'Server' in item:
                tech_keywords.add(item['Server'].lower())
        
        if tech_keywords:
            st.write(f"🔧 Gefundene Technologien: {', '.join(list(tech_keywords)[:5])}")

        # Phase 2: OSINT People
        status_text.text("👥 Phase 2/5: Searching Employees via OSINT...")
        st.session_state['scan_status'] = "👥 Phase 2/5: Searching Employees via OSINT..."
        st.session_state['scan_progress'] = 25
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
                entity['source'] = str(person.get('Quelle') or person.get('Source') or "OSINT")
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

        if hasattr(self.people_scanner, 'discovered_people'):
            for person in self.people_scanner.discovered_people:
                name_lower = person['name'].lower()
                exists = False
                
                for entity in found_entities_from_osint:
                    if entity.get('name', '').lower() == name_lower:
                        exists = True
                        if person.get('url') and 'url' in entity:
                            if isinstance(entity.get('url'), list):
                                if person['url'] not in entity['url']:
                                    entity['url'].append(person['url'])
                            else:
                                existing_url = entity.get('url')
                                if existing_url != person['url']:
                                    entity['url'] = [existing_url, person['url']] if existing_url else [person['url']]
                        break
                
                if not exists:
                    found_entities_from_osint.append({
                        'name': person['name'],
                        'url': person.get('url'),
                        'source': person.get('source', 'Discovered from page'),
                        'snippet': f"Discovered from {person.get('source', 'unknown page')}"
                    })
                    st.caption(f"  ✅ Added discovered person: {person['name']}")

        st.write(f"👤 Gefundene Entitäten aus OSINT: {len(found_entities_from_osint)}")
        if found_entities_from_osint:
            with_urls = sum(1 for e in found_entities_from_osint if e.get('url'))
            st.caption(f"   📎 Davon mit URLs: {with_urls}")

        # Phase 3: Social Media
        status_text.text("🔄 Phase 3/5: Searching for Social Media Profiles...")
        st.session_state['scan_status'] = "🔄 Phase 3/5: Searching for Social Media Profiles..."
        st.session_state['scan_progress'] = 40
        progress_bar.progress(40)

        social_entities = []
        if not df_people_osint.empty:
            for _, person in df_people_osint.head(3).iterrows():
                name = person.get('Name', '')
                if name:
                    social_hits = self.social_scanner.search_entity_globally(name, self.target_company)
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
            st.session_state['scan_status'] = f"🔍 Phase 4/5: GitHub API Scan (Budget: {api_budget} Calls)..."
            st.session_state['scan_progress'] = 60
            progress_bar.progress(60)
            
            self.code_scanner.use_web_always = False
            self.code_scanner.using_fallback = False
        else:
            status_text.text("🔍 Phase 4/5: GitHub Web-Fallback Scan...")
            st.session_state['scan_status'] = "🔍 Phase 4/5: GitHub Web-Fallback Scan..."
            st.session_state['scan_progress'] = 60
            progress_bar.progress(60)
            
            self.code_scanner.use_web_always = True
            self.code_scanner.using_fallback = True
            used_web_fallback = True
        
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
                    st.success(f"✅ Web-Fallback Scan erfolgreich: {len(df_code)} Repos, {len(df_github_users)} Nutzer")
                else:
                    st.success(f"✅ API Scan erfolgreich: {len(df_code)} Repos, {len(df_github_users)} Nutzer")
            else:
                st.warning("⚠️ GitHub Scan lieferte keine Ergebnisse. Versuche Cache...")
                cached_data = self.cache_manager.load(self.cache_key)
                if cached_data:
                    st.info("📦 Verwende gecachte GitHub-Daten...")
                    df_code = cached_data['code']
                    is_cached = True
                else:
                    st.warning("ℹ️ Keine gecachten GitHub-Daten vorhanden.")
                    
        except Exception as e:
            st.error(f"❌ GitHub Scan fehlgeschlagen: {e}")
            cached_data = self.cache_manager.load(self.cache_key)
            if cached_data:
                st.info("📦 Verwende gecachte GitHub-Daten...")
                df_code = cached_data['code']
                is_cached = True

        progress_bar.progress(80)

        # Phase 5: Pivot & Combine 
        status_text.text("🕵️ Phase 5/5: Analyze and combine findings...")
        st.session_state['scan_status'] = "🕵️ Phase 5/5: Analyze and combine findings..."
        st.session_state['scan_progress'] = 80
        progress_bar.progress(80)
        
        df_github_pivoted = pd.DataFrame()

        if not df_github_users.empty:
            login_col = next(
                (c for c in df_github_users.columns if c.lower() in ['login', 'username', 'user', 'name', 'github_user']),
                df_github_users.columns[0]
            )
            cache_key = "github_scan_" + str(sorted(df_github_users[login_col].tolist()))          
            if cache_key not in st.session_state:
                st.write(f"Analysiere {min(5, len(df_github_users))} GitHub-Entitäten auf Social Media...")
                
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

        # Combine all data
        dfs_to_concat = []
        
        if not df_github_pivoted.empty:
            dfs_to_concat.append(df_github_pivoted)
        
        if found_entities_from_osint:
            osint_list = []
            existing_names = set()
            if dfs_to_concat:
                for df in dfs_to_concat:
                    if not df.empty and 'Name' in df.columns:
                        existing_names.update([str(n).lower() for n in df['Name'].dropna() if pd.notna(n)])
            
            for entity in found_entities_from_osint[:10]:
                name = entity.get('name', '')
                name_lower = name.lower()
                
                if name_lower in existing_names:
                    st.caption(f"   ⏭️ Skipping {name} (already found via GitHub)")
                    continue
                
                if name:
                    links = []
                    if entity.get('url'):
                        if isinstance(entity['url'], list):
                            links.extend(entity['url'])
                        else:
                            links.append(str(entity['url']))
                    
                    if entity.get('scraped_social_links'):
                        for link in entity['scraped_social_links']:
                            if link not in links:
                                links.append(link)
                    
                    primary_url = None
                    if links:
                        linkedin_urls = [l for l in links if 'linkedin.com' in l.lower()]
                        primary_url = linkedin_urls[0] if linkedin_urls else links[0]
                    
                    emails = entity.get('emails', [])
                    
                    details = entity.get('snippet', '')
                    if entity.get('source'):
                        details = f"{details} | Source: {entity['source']}" if details else f"Source: {entity['source']}"
                    
                    osint_list.append({
                        "Name": name,
                        "Username": entity.get('username', 'N/A'),
                        "Source": entity.get('source', 'OSINT Search'),
                        "Status": "OSINT Found",
                        "Offizielle_Firma": self.target_company,
                        "Gefundene_Links": links,
                        "Emails": emails,
                        "URL": primary_url,
                        "Details": details[:200] if details else "Found via OSINT search",
                        "Real_Name": name
                    })
                    
                    existing_names.add(name_lower)
            
            if osint_list:
                st.caption(f"📋 Added {len(osint_list)} new OSINT-only entries with enriched links")
                dfs_to_concat.append(pd.DataFrame(osint_list))
        
        if dfs_to_concat:
            df_p_final = pd.concat(dfs_to_concat, ignore_index=True)
            if 'Name' in df_p_final.columns:
                df_p_final = df_p_final.drop_duplicates(subset=['Name'])
            for col in ['Emails', 'Gefundene_Links', 'Real_Name']:
                if col not in df_p_final.columns:
                    df_p_final[col] = None if col != 'Gefundene_Links' else []
        else:
            df_p_final = pd.DataFrame()

        progress_bar.progress(100)
        status_text.text("✅ Scan abgeschlossen!")
        st.session_state['scan_status'] = "✅ Scan abgeschlossen!"
        st.session_state['scan_progress'] = 100
        st.session_state['is_scanning'] = False
        
        if df_p_final.empty and df_code.empty:
            st.warning("⚠️ Keine Daten gefunden.")
        else:
            total_links = 0
            total_emails = 0
            if not df_p_final.empty:
                total_links = df_p_final['Gefundene_Links'].apply(len).sum() if 'Gefundene_Links' in df_p_final else 0
                total_emails = df_p_final['Emails'].apply(len).sum() if 'Emails' in df_p_final else 0
            
            st.success(f"✅ Scan abgeschlossen! Gefunden: {len(df_p_final)} Mitarbeiter, {len(df_code)} Repositories, {total_links} Links, {total_emails} Emails")
        
        if is_cached:
            st.info("📦 Hinweis: Einige Daten stammen aus dem Cache")

        if not is_cached and not df_code.empty:
            self.cache_manager.save(self.cache_key, {
                'people': df_p_final, 
                'infra': infra_combined, 
                'code': df_code,
                'subdomains': subdomains, 
                'enrichment': enrichment
            })

        st.markdown("---")
        st.subheader("🔐 Breach Detection")
        
        with st.expander("Check emails for data breaches", expanded=False):
            st.info("Checking emails against Have I Been Pwned database...")
            breach_results = self.check_employee_breaches(df_p_final)
            
            if breach_results:
                st.session_state['breach_results'] = breach_results

        return df_p_final, infra_combined, df_code, subdomains, enrichment, is_cached, safe_search, breach_results

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

    def _combine_people_results(self, df_osint, df_github):
        all_people = []
        
        if not df_osint.empty:
            for _, person in df_osint.iterrows():
                links = []
                
                if 'URL' in person and pd.notna(person['URL']):
                    links.append(str(person['URL']))
                if 'url' in person and pd.notna(person['url']):
                    links.append(str(person['url']))
                if 'profile_url' in person and pd.notna(person['profile_url']):
                    links.append(str(person['profile_url']))
                if 'Link' in person and pd.notna(person['Link']):
                    links.append(str(person['Link']))
                
                if 'Gefundene_Links' in person_dict:
                    links = person_dict['Gefundene_Links']
                    if isinstance(links, list):
                        flat_links = []
                        for link in links:
                            if isinstance(link, list):
                                flat_links.extend([str(l).strip().replace(' ', '').replace('\n', '') 
                                                for l in link if l and str(l) not in flat_links])
                            elif link and str(link) not in flat_links:
                                clean_link = str(link).strip().replace(' ', '').replace('\n', '')
                                flat_links.append(clean_link)
                        person_dict['Gefundene_Links'] = flat_links
                
                person_dict = {
                    "Name": person.get('Name', 'Unknown'),
                    "Username": person.get('Username', 'N/A'),
                    "Source": person.get('Source', 'OSINT Search'),
                    "Status": person.get('Status', 'OSINT Found'),
                    "Offizielle_Firma": person.get('Company', self.target_company),
                    "Gefundene_Links": links,
                    "URL": person.get('URL') or person.get('url') or (links[0] if links else None),
                    "Details": person.get('Details', 'Found via OSINT search')
                }
                all_people.append(person_dict)
        
        if not df_github.empty:
            existing_names = {p['Name'].lower() for p in all_people}
            for _, person in df_github.iterrows():
                person_dict = person.to_dict()
                name_lower = person_dict.get('Name', '').lower()
                
                if name_lower and name_lower not in existing_names:
                    if 'Gefundene_Links' in person_dict:
                        links = person_dict['Gefundene_Links']
                        if isinstance(links, list):
                            flat_links = []
                            for link in links:
                                if isinstance(link, list):
                                    flat_links.extend([str(l) for l in link if l and str(l) not in flat_links])
                                elif link and str(link) not in flat_links:
                                    flat_links.append(str(link))
                            person_dict['Gefundene_Links'] = flat_links
                    
                    all_people.append(person_dict)
                    existing_names.add(name_lower)
        
        for person in all_people:
            if 'Gefundene_Links' not in person or not isinstance(person['Gefundene_Links'], list):
                person['Gefundene_Links'] = []
            
            if not person.get('URL') and person.get('Gefundene_Links'):
                person['URL'] = person['Gefundene_Links'][0]
        
        return pd.DataFrame(all_people) if all_people else pd.DataFrame()

def main():
    st.sidebar.title("🛡️ TRUSTEQ OSINT")
    target_company = st.sidebar.text_input("Firmenname", value="trusteq")
    target_domain = st.sidebar.text_input("Domain", value="trusteq.de")
    
    with st.sidebar.expander("🔑 API Keys"):
        github_token = st.text_input("GitHub Token", type="password", 
                                    help="Von https://github.com/settings/tokens (repo + user scopes)")
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
    page = st.sidebar.radio("Navigation", ["Dashboard Übersicht", "Gefundene Mitarbeiter", "Code Leaks", "Breach Results"])

    collector = OSINTCollector(target_company, target_domain, github_token, hibp_key)

    if st.sidebar.button("Live Scan Starten"):
        # Reset scan state
        st.session_state['is_scanning'] = False
        st.session_state['scan_results'] = None
        
        with st.spinner(f"Scan runs (dauert 2-4 minutes)..."):
            df_p, infra, df_c, subs, enrich, is_cached, safe_search, breach_results = collector.run_full_scan()
            
            st.session_state['scan_results'] = {
                'people': df_p if not df_p.empty else pd.DataFrame(), 
                'infra': infra, 
                'code': df_c, 
                'subdomains': subs, 
                'enrichment': enrich,
                'timestamp': datetime.now().strftime('%H:%M:%S'),
                'safe_search': safe_search,
                'is_cached': is_cached,
                'breach_results': breach_results
            }
            
            if is_cached:
                st.sidebar.warning("⚠️ Showing cached data")
            else:
                st.sidebar.success("✅ Scan complete!")
            
            st.rerun()

    results = st.session_state.get('scan_results', None)
    
    if page == "Dashboard Übersicht":
        render_dashboard(results, collector)
    elif page == "Gefundene Mitarbeiter":
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
        status = st.session_state.get('scan_status', "Initialisiere...")
        
        st.progress(progress / 100)
        st.info(f"**Current Phase:** {status}")
        st.markdown("---")
        st.info("Scan runs in the background. ")
        
        time.sleep(2)
        st.rerun()
        return
    
    # If not scanning, show the dashboard as before
    if not results:
        st.info("Bitte Scan starten.")
        return

    if results.get('is_cached'):
        st.warning(f"⚠️ **ACHTUNG:** API Rate Limit aktiv. Zeige Daten vom letzten erfolgreichen Scan ({results.get('timestamp')}).")

    st.title(f"🛡️ Surface Overview: {collector.target_company}")
    st.markdown(f"**Domain:** {collector.target_domain} | **Letzter Scan:** {results.get('timestamp')}")
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
        st.metric("Mitarbeiter", len(results['people']) if not results['people'].empty else 0)
        st.metric("Repos", len(results['code']) if not results['code'].empty else 0)
    with c3:
        st.subheader("Risk Breakdown")
        st.caption(f"Infrastructure: {risk_data['breakdown']['Infrastructure']}/100")
        st.caption(f"Code/GitHub: {risk_data['breakdown']['Code']}/100")
        st.caption(f"People OSINT: {risk_data['breakdown']['People']}/100")

    st.divider()
    if results['subdomains']:
        st.subheader("⚠️ Kritische Subdomains")
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
                st.write(f"**Company:** {person.get('Offizielle_Firma', 'Unknown')}")
                
                if 'Details' in person and person['Details'] and pd.notna(person['Details']):
                    details = str(person['Details'])
                    details = re.sub(r'\s+', ' ', details)
                    details = details.replace(' @ ', '@').replace(' . ', '.')
                    st.info(details)
                
                links = person.get('Gefundene_Links', [])
                if links and len(links) > 0:
                    st.write("**🔗 Gefundene Profile & Mentions:**")
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
            st.warning("⚠️ Anzeige basiert auf Cache-Daten.")
        
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
        st.info("Keine Repositories gefunden.")

def render_breach_page(results):
    st.subheader("Breach Results")
    
    breach_results = results.get('breach_results', {}) if results else {}

    if not breach_results:
        st.info("No breach data available. Run a scan with breach checking enabled.")
        return
    
    st.warning(f"⚠️ Found {len(breach_results)} compromised email(s)!")
    
    for email, data in breach_results.items():
        with st.expander(f"📧 {email} - {data['person']}"):
            st.write(f"**Person:** {data['person']}")
            st.write(f"**Email:** {email}")
            st.write(f"**Number of Breaches:** {data['count']}")
            st.write(f"**Detection Method:** {data['method']}")
            
            if data['breaches']:
                st.write("**Breaches:**")
                for breach in data['breaches']:
                    st.write(f"- {breach}")
            
            st.markdown(f"[🔍 View on Have I Been Pwned](https://haveibeenpwned.com/account/{email})")

if __name__ == "__main__":
    main()
