import requests, pandas as pd, re, base64, time, random, json
from typing import Optional, Dict, List, Set, Any
from dataclasses import dataclass
from datetime import datetime
from bs4 import BeautifulSoup
from html_scraper import HTMLScraper
@dataclass
class ScanResult:
    repos: pd.DataFrame; users: pd.DataFrame; keywords: Set[str]
    entities: List[Dict[str, str]]; search_terms: Set[str]; timestamp: str

class CodeScanner:
    def __init__(self, target_company, github_token=None, max_iterations=2, verbose=True):
        self.target_company = target_company
        self.base_url = "https://api.github.com"
        self.github_token = github_token
        self.session = requests.Session()
        self.html_scraper = HTMLScraper(delay=1.0)
        if github_token:
            self.session.headers.update({'Authorization': f'token {github_token}'})
            if verbose:
                print(f"✓ GitHub Token configured (starts with: {github_token[:4]}...)")
        
        self.web_session = requests.Session()
        self.web_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5', 'DNT': '1', 'Connection': 'keep-alive'
        })
        
        self.max_iterations = max_iterations
        self.verbose = verbose
        self.all_repos = pd.DataFrame()
        self.all_users = pd.DataFrame()
        self.found_keywords = set()
        self.found_entities = []
        self.searched_terms = set()
        self.iteration_count = 0
        self.iteration_history = []
        self.processed_repos = set()
        self.api_failures = 0
        self.max_api_failures = 3
        self.using_fallback = False
        self.use_web_always = True
        
        # API Budget tracking
        self.api_calls_used = 0
        self.max_api_calls = 250  # Default budget
        self.start_time = None

    def _log(self, message: str):
        if self.verbose:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def _check_timeout(self):
        # Check if we've exceeded reasonable scan time (5 minutes)
        if self.start_time:
            elapsed = (datetime.now() - self.start_time).total_seconds() / 60
            if elapsed > 5:  # 5 minute timeout
                self._log(f"⏰ Timeout reached after {elapsed:.1f} minutes")
                return True
        return False

    def _check_rate_limit_before_request(self, estimated_cost=1):
        # Check if we have enough API budget
        if self.api_calls_used + estimated_cost > self.max_api_calls:
            self._log(f"⚠️ API Budget erschöpft ({self.max_api_calls} calls)")
            self.using_fallback = True
            return False
        return True
    # TODO not executed atm - add some test calls to this method and check if the logging works and if the fallback gets activated
    def get_iteration_report(self) -> str:
        if not self.iteration_history:
            return "⚠️ No Iteration executed yet."
        
        report = "\n" + "=" * 75 + "\n📊 ITERATIVE SEARCH REPORT\n" + "=" * 75 + "\n"
        for d in self.iteration_history:
            report += f"\n🔄 Iteration {d['iteration']} (max {self.max_iterations}):\n"
            report += f"   ├─ Search Term: '{d['search_term']}'\n"
            report += f"   ├─ 📦 Repos: +{d['repos_found']} (Gesamt: {d['total_repos']})\n"
            report += f"   ├─ 👥 Users: +{d['users_found']} (Gesamt: {d['total_users']})\n"
            report += f"   ├─ 🔑 Keywords: +{d['keywords_found']} (Gesamt: {d['total_keywords']})\n"
            report += f"   ├─ 👤 Entities: +{d['entities_found']} (Gesamt: {d['total_entities']})\n"
            
            api_count = d.get('api_results', 0)
            web_count = d.get('web_results', 0)
            if api_count > 0 or web_count > 0:
                report += f"   ├─ 🌐 Sources: API: {api_count} | Web: {web_count}\n"
            
            report += f"   └─ {'✓ ' + str(len(d['queued_terms'])) + ' new terms' if d['queued_terms'] else '⏹️ No new terms'}\n"
        
        report += f"\n{'='*75}\n✓ FINAL RESULTS\n{'='*75}\n"
        report += f"Iterations: {self.iteration_count}/{self.max_iterations}\n"
        report += f"Repos: {len(self.all_repos)} | Users: {len(self.all_users)} | Keywords: {len(self.found_keywords)}\n"
        report += f"Entities: {len(self.found_entities)} | Search Terme: {len(self.searched_terms)}\n"
        report += f"API Calls: {self.api_calls_used}/{self.max_api_calls}\n"
        
        mode = "🌐 Web-Fallback" if self.using_fallback else "🔑 API + Web Hybrid"
        report += f"Mode: {mode}\n"
        report += f"Timestamp: {datetime.now().isoformat()}\n"
        
        return report + "=" * 75

    def _api_request(self, url, timeout=10):
        if not self._check_rate_limit_before_request():
            return None
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout)
                self.api_calls_used += 1
                
                # Handle API Rate Limiting / Abuse detection
                if response.status_code in [403, 429]:
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    self._log(f"⚠️ GitHub Rate Limit hit (403/429). Backing off for {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    continue # Retry
                
                # Check if token is valid
                if response.status_code == 401:
                    self._log("❌ Token invalid or expired")
                    self.github_token = None
                    self.session.headers.pop('Authorization', None)
                    self.using_fallback = True
                
                return response
            except Exception as e:
                self._log(f"❌ API request failed: {e}")
                return None
        return None

    def _web_request(self, url, timeout=10):
        try:
            time.sleep(random.uniform(1, 2))
            return self.web_session.get(url, timeout=timeout)
        except:
            return None

    def scan_specific_organization(self, org_name: str = None) -> pd.DataFrame:
        org_name = org_name or self.target_company
        try:
            res = self._api_request(f"{self.base_url}/orgs/{org_name}/repos?per_page=100&sort=updated")
            if res and res.status_code == 200:
                results = [{
                    'repo_name': r['full_name'], 'display_name': r['name'], 'url': r['html_url'],
                    'description': r.get('description', ''), 'last_update': r.get('updated_at', ''),
                    'language': r.get('language', ''), 'stars': r.get('stargazers_count', 0),
                    'forks': r.get('forks_count', 0), 'source': 'organization_scan', 'risk_score': 5
                } for r in res.json()]
                self._log(f"✓ {len(results)} Repos in '{org_name}' gefunden")
                return pd.DataFrame(results)
        except Exception as e:
            self._log(f"❌ Fehler: {e}")
        return pd.DataFrame()

    def search_github_web(self, query: str, search_type: str = "repositories") -> List[Dict]:
        url_map = {'repositories': 'repositories', 'code': 'code', 'users': 'users'}
        if search_type not in url_map:
            return []
        
        try:
            url = f"https://github.com/search?q={requests.utils.quote(query)}&type={url_map[search_type]}"
            self._log(f"🌐 Web-Suche: {search_type} mit '{query}'")
            res = self._web_request(url)
            
            if not res or res.status_code != 200:
                return []
            
            soup = BeautifulSoup(res.text, 'html.parser')
            items = soup.select('.repo-list-item, .code-list-item, .user-list-item, .Box-row, [data-testid="results-list"] > div')
            results = []
            
            for item in items[:15]:
                try:
                    link = item.select_one('a[href*="/"]')
                    if not link:
                        continue
                    
                    href = link.get('href', '')
                    if not href.startswith('/'):
                        continue
                    
                    if search_type == "repositories":
                        full_name = href.strip('/')
                        desc = item.select_one('.mb-1, .col-9, .description')
                        results.append({
                            'repo_name': full_name, 'display_name': full_name.split('/')[-1],
                            'url': f"https://github.com{href}", 'description': desc.text.strip() if desc else '',
                            'source': 'web_search', 'risk_score': 25, 'search_type': 'repository'
                        })
                    elif search_type == "code":
                        parts = href.split('/')
                        repo_name = f"{parts[1]}/{parts[2]}" if len(parts) >= 3 else "unknown"
                        results.append({
                            'file_path': link.text.strip(), 'repo_name': repo_name,
                            'url': f"https://github.com{href}", 'source': 'web_search',
                            'search_term': query, 'search_type': 'code', 'risk_score': 30
                        })
                    elif search_type == "users":
                        username = href.strip('/')
                        profile_url = f"https://github.com/{username}"
                        # Check global cache before even creating the user dict
                        if hasattr(self, 'scraped_urls_global') and profile_url in self.scraped_urls_global:
                            continue  # Skip adding this user entirely
                        # Add to cache
                        if hasattr(self, 'scraped_urls_global'):
                            self.scraped_urls_global.add(profile_url)

                        name = item.select_one('.f5, .text-normal')
                        bio = item.select_one('.user-list-bio, .col-8')
                        company = item.select_one('.user-list-company, [itemprop="worksFor"]')
                        company_text = company.text.strip() if company else "Nicht angegeben"
                        
                        # Create user dict with placeholder real_name
                        user_dict = {
                            'Username': username, 
                            'Real_Name': name.text.strip() if name else username,
                            'Bio': bio.text.strip() if bio else "", 
                            'Company_Field': company_text,
                            'GitHub_URL': f"https://github.com{href}", 
                            'source': 'web_search',
                            'Is_Verified_Employee': self.target_company.lower() in company_text.lower(),
                            'search_type': 'user'
                        }
                        
                        # ALWAYS try to scrape the profile page for better data (even without API)
                        try:
                            scraped_data = self.html_scraper.scrape(f"https://github.com/{username}")
                            if scraped_data and scraped_data.get('real_name'):
                                scraped_name = scraped_data['real_name']
                                if scraped_name and scraped_name != username and len(scraped_name) > 2:
                                    user_dict['Real_Name'] = scraped_name
                                    self._log(f"      ✅ Enhanced web result for {username} with scraped name: '{scraped_name}'")
                        except Exception as e:
                            pass  # Silently continue if scraping fails
                        
                        results.append(user_dict)
                except:
                    continue
            
            self._log(f"✅ {len(results)} Ergebnisse für '{query}'")
            return results
        except Exception as e:
            self._log(f"❌ Web-Suchfehler: {e}")
            return []

    def search_github_users_web(self, name: str) -> List[Dict]:
        try:
            url = f"https://github.com/search?q={requests.utils.quote(name + ' in:fullname')}&type=users"
            self._log(f"🌐 Web-Namenssuche: '{name}'")
            res = self._web_request(url)
            
            if not res or res.status_code != 200:
                return []
            
            soup = BeautifulSoup(res.text, 'html.parser')
            results = []
            for card in soup.select('.user-list-item, .Box-row, [data-testid="results-list"] > div')[:10]:
                try:
                    link = card.select_one('a[href*="/"]')
                    if not link or not link.get('href', '').startswith('/'):
                        continue
                    
                    username = link.get('href', '').strip('/')
                    name_elem = card.select_one('.f5')
                    bio_elem = card.select_one('.user-list-bio')
                    company_elem = card.select_one('.user-list-company')
                    company_text = company_elem.text.strip() if company_elem else "Nicht angegeben"
                    
                    # Create user dict with placeholder real_name
                    user_dict = {
                        'Username': username, 
                        'Real_Name': name_elem.text.strip() if name_elem else name,
                        'Bio': bio_elem.text.strip() if bio_elem else "", 
                        'Company_Field': company_text,
                        'GitHub_URL': f"https://github.com/{username}", 
                        'source': 'web_name_search',
                        'Is_Verified_Employee': self.target_company.lower() in company_text.lower()
                    }
                    
                    # ALWAYS try to scrape the profile page for better data
                    try:
                        scraped_data = self.html_scraper.scrape(f"https://github.com/{username}")
                        if scraped_data and scraped_data.get('real_name'):
                            scraped_name = scraped_data['real_name']
                            if scraped_name and scraped_name != username and len(scraped_name) > 2:
                                user_dict['Real_Name'] = scraped_name
                                self._log(f"      ✅ Enhanced web name search for {username} with scraped name: '{scraped_name}'")
                    except Exception as e:
                        pass
                    
                    results.append(user_dict)
                except:
                    continue
            
            self._log(f"✅ {len(results)} Users für '{name}'")
            return results
        except Exception as e:
            self._log(f"❌ Fehler: {e}")
            return []

    def search_repositories_web(self, query: str) -> pd.DataFrame:
        results = self.search_github_web(query, "repositories")
        return pd.DataFrame(results) if results else pd.DataFrame()

    def _merge_results(self, new_df, existing_df, key_col='url', id_col=None):
        if new_df.empty:
            return existing_df
        if existing_df.empty:
            return new_df
        
        id_col = id_col or key_col
        existing = set(existing_df.get(id_col, [])) if not existing_df.empty else set()
        new = new_df[~new_df[id_col].isin(existing)] if id_col in new_df.columns else new_df
        return pd.concat([existing_df, new], ignore_index=True) if not new.empty else existing_df

    def search_users(self, company_name: str = None, keyword: str = None, max_results: int = 30) -> pd.DataFrame:
        query = keyword or company_name or self.target_company
        all_results, api_results, web_results = [], [], []
        
        if not self.using_fallback:
            try:
                res = self._api_request(f"{self.base_url}/search/users?q={query}&per_page={min(max_results, 100)}")
                if res and res.status_code == 200:
                    for item in res.json().get('items', []):
                        if user_info := self.verify_user_identity(item['login']):
                            user_info['source'] = 'api_user_search'
                            api_results.append(user_info)
                    self._log(f"✓ API: {len(api_results)} Users")
                    all_results.extend(api_results)
            except Exception as e:
                self._log(f"❌ API-Fehler: {e}")
        
        if self.use_web_always or self.using_fallback:
            web_results = self.search_github_web(query, "users")
            all_results.extend(web_results)
        
        if all_results:
            df = pd.DataFrame(all_results)
            if 'Username' in df.columns:
                df = df.drop_duplicates(subset=['Username'], keep='first')
            self._log(f"📊 Users gesamt: {len(df)} (API: {len(api_results)}, Web: {len(web_results)})")
            return df
        return pd.DataFrame()

    def search_users_by_name(self, name: str, max_results: int = 10) -> pd.DataFrame:
        all_results, api_results, web_results = [], [], []
        
        if not self.using_fallback:
            try:
                res = self._api_request(f"{self.base_url}/search/users?q={name} in:name&per_page={min(max_results, 100)}")
                if res and res.status_code == 200:
                    for item in res.json().get('items', []):
                        if user_info := self.verify_user_identity(item['login']):
                            user_info.update({'Found_By': f"api_name_search:{name}", 'source': 'api_name_search'})
                            api_results.append(user_info)
                    self._log(f"✓ API: {len(api_results)} Users")
                    all_results.extend(api_results)
            except Exception as e:
                self._log(f"❌ API-Fehler: {e}")
        
        if self.use_web_always or self.using_fallback:
            web_results = self.search_github_users_web(name)
            all_results.extend(web_results)
        
        if all_results:
            df = pd.DataFrame(all_results).drop_duplicates(subset=['Username'], keep='first')
            self._log(f"📊 Users gesamt: {len(df)}")
            return df
        return pd.DataFrame()

    def get_contributors(self, repo_full_name: str, max_contributors: int = 20) -> List[Dict]:
        all_contributors = []
        
        if not self.using_fallback and self._check_rate_limit_before_request():
            try:
                res = self._api_request(f"{self.base_url}/repos/{repo_full_name}/contributors?per_page={max_contributors}")
                if res and res.status_code == 200:
                    for c in res.json():
                        if username := c.get('login'):
                            if user_info := self.verify_user_identity(username):
                                user_info.update({
                                    'Contributions': c.get('contributions', 0), 'Source_Repo': repo_full_name,
                                    'Found_By': f"contributor_of:{repo_full_name}", 'source': 'api_contributor'
                                })
                                all_contributors.append(user_info)
                    self._log(f"✓ API: {len(all_contributors)} Mitwirkende")
            except Exception as e:
                self._log(f"❌ API-Fehler: {e}")
        
        if self.use_web_always or self.using_fallback:
            try:
                res = self._web_request(f"https://github.com/{repo_full_name}")
                if res and res.status_code == 200:
                    soup = BeautifulSoup(res.text, 'html.parser')
                    existing = {c.get('Username') for c in all_contributors}
                    
                    for link in soup.select('a[data-hovercard-type="user"]')[:max_contributors]:
                        if href := link.get('href', ''):
                            username = href[1:] if href.startswith('/') else href
                            if username not in existing:
                                # Try to get real name by scraping
                                real_name = username
                                try:
                                    scraped_data = self.html_scraper.scrape(f"https://github.com/{username}")
                                    if scraped_data and scraped_data.get('real_name'):
                                        scraped_name = scraped_data['real_name']
                                        if scraped_name and scraped_name != username and len(scraped_name) > 2:
                                            real_name = scraped_name
                                except Exception:
                                    pass
                                
                                all_contributors.append({
                                    'Username': username, 'GitHub_URL': f"https://github.com/{username}",
                                    'Real_Name': real_name, 'Source_Repo': repo_full_name,
                                    'Found_By': f"web_contributor_of:{repo_full_name}",
                                    'source': 'web_contributor', 'Is_Verified_Employee': 'unknown'
                                })
                    self._log(f"✓ Web: zusätzliche Mitwirkende")
            except Exception as e:
                self._log(f"❌ Web-Fehler: {e}")
        
        return all_contributors

    def verify_user_identity(self, username):

        # First, check if we've already scraped this profile
        profile_url = f"https://github.com/{username}"
        
        if hasattr(self, 'scraped_urls_global') and profile_url in self.scraped_urls_global:
            self._log(f"  ⏭️ Skipping {username} (already scraped globally)")
            # Try to return cached data
            if not self.all_users.empty and username in self.all_users['Username'].values:
                return self.all_users[self.all_users['Username'] == username].iloc[0].to_dict()
            return None
        
        # Add to global cache
        if hasattr(self, 'scraped_urls_global'):
            self.scraped_urls_global.add(profile_url)

        try:
            # First: Try to get the real name by scraping the GitHub profile page
            profile_url = f"https://github.com/{username}"
            self._log(f"🔍 Scraping GitHub profile for {username} to get real name...")
            
            # Use the HTML scraper to get the real name
            scraped_data = self.html_scraper.scrape(profile_url)
            
            # Initialize variables
            real_name = None
            bio = ""
            company_field = ""
            github_url = None
            twitter_username = None
            blog = None
            emails = []
            social_links = []
            profile_insights = []
            
            # Get data from scraper FIRST (this is the most reliable for real names)
            if scraped_data:
                scraped_name = scraped_data.get('real_name')
                if scraped_name and scraped_name != username and len(scraped_name) > 2:
                    real_name = scraped_name
                    self._log(f"✅ Using scraped real name for {username}: '{real_name}'")
                
                # Also get any other data from scraper
                if not company_field and scraped_data.get('company'):
                    company_field = scraped_data['company']
                if scraped_data.get('emails'):
                    emails = scraped_data['emails']
                if scraped_data.get('social_links'):
                    social_links = scraped_data['social_links']
            
            # THEN try API for additional data (but don't override real_name if we already have it)
            url = f"{self.base_url}/users/{username}"
            res = self.session.get(url, timeout=5)
            
            if res.status_code == 200:
                data = res.json()
                bio = data.get('bio', '') or ""
                
                # Only use API company if we don't have one from scraper
                if not company_field:
                    company_field = data.get('company', '') or ""
                
                github_url = data.get('html_url')
                twitter_username = data.get('twitter_username')
                blog = data.get('blog')
                
                # Only use API name if we don't have a scraped name
                if not real_name:
                    api_name = data.get('name')
                    if api_name and api_name != username and len(api_name) > 2:
                        real_name = api_name
                        self._log(f"✅ Using API name for {username}: '{real_name}'")
            
            # Final fallback to username
            if not real_name:
                real_name = username
                self._log(f"⚠️ Using username as fallback for {username}")
            
            # Deep scan profile README for additional data
            profile_data = self.deep_scan_profile_text(username)
            
            # Merge all data sources
            all_links = [github_url] if github_url else []
            if twitter_username:
                all_links.append(f"https://twitter.com/{twitter_username}")
            if blog and blog not in all_links:
                all_links.append(blog)
            if social_links:
                all_links.extend(social_links)
            if profile_data.get('social_links'):
                all_links.extend(profile_data['social_links'])
            
            all_links = list(set(all_links))
            
            # Combine emails
            all_emails = list(set(emails + profile_data.get('emails', [])))
            
            # Check if employee
            is_employee = False
            if self.target_company.lower() in bio.lower() or self.target_company.lower() in company_field.lower():
                is_employee = True
            
            self._log(f"📊 User {username}: FINAL name='{real_name}', Found {len(all_links)} links, {len(all_emails)} emails")
            
            return {
                "Username": username,
                "Real_Name": real_name,
                "Company_Field": company_field or "Nicht angegeben",
                "Is_Verified_Employee": is_employee,
                "Bio": bio or profile_data.get("profile_bio", ""),
                "Gefundene_Links": all_links,
                "Emails": all_emails,
                "URL": github_url,
                "Profile_Insights": profile_data.get("detected_keywords", [])
            }
        except Exception as e:
            self._log(f"❌ Fehler bei der Verifizierung von Users {username}: {e}")
            return None

    def deep_scan_profile_text(self, username):
        found_data = {
            "social_links": [], 
            "detected_keywords": [],
            "emails": [],
            "profile_bio": ""
        }
        
        try:
            readme_urls = [
                f"{self.base_url}/repos/{username}/{username}/contents/README.md",
                f"{self.base_url}/repos/{username}/{username}/contents/README",
                f"{self.base_url}/repos/{username}/.github/contents/README.md",
                f"{self.base_url}/repos/{username}/.github/contents/README",
                f"{self.base_url}/repos/{username}/profile/contents/README.md"
            ]
            
            content_text = None
            
            for readme_url in readme_urls:
                try:
                    res = self.session.get(readme_url, timeout=5)
                    if res.status_code == 200:
                        content_encoded = res.json().get('content', '')
                        if content_encoded:
                            content_text = base64.b64decode(content_encoded).decode('utf-8', errors='ignore')
                            self._log(f"✓ Found profile README for {username}")
                            break
                except Exception:
                    continue
            
            if content_text:
                # Markdown links
                markdown_link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
                markdown_links = re.findall(markdown_link_pattern, content_text)
                
                for link_text, link_url in markdown_links:
                    link_url = link_url.strip()
                    if self._is_social_media_url(link_url) and link_url not in found_data["social_links"]:
                        found_data["social_links"].append(link_url)
                
                # Raw URLs
                url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[-\w$.+!*\'(),;:@&=?/~#%]*)?'
                raw_urls = re.findall(url_pattern, content_text)
                
                for url in raw_urls:
                    if self._is_social_media_url(url) and url not in found_data["social_links"]:
                        found_data["social_links"].append(url)
                
                # HTML links
                html_link_pattern = r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"[^>]*>'
                html_links = re.findall(html_link_pattern, content_text, re.IGNORECASE)
                
                for link_url in html_links:
                    if self._is_social_media_url(link_url) and link_url not in found_data["social_links"]:
                        found_data["social_links"].append(link_url)
                
                # Social handles
                social_handles = self._extract_social_handles(content_text)
                for platform, handle in social_handles.items():
                    if handle:
                        constructed_url = self._construct_social_url(platform, handle)
                        if constructed_url and constructed_url not in found_data["social_links"]:
                            found_data["social_links"].append(constructed_url)
                
                # Emails
                email_patterns = [
                    r'[\w\.-]+@[\w\.-]+\.\w+',
                    r'[\w\.-]+\[\s*at\s*\][\w\.-]+\[\s*dot\s*]\w+',
                    r'[\w\.-]+\s*\(at\)\s*[\w\.-]+\s*\(dot\)\s*\w+',
                ]
                
                for pattern in email_patterns:
                    emails = re.findall(pattern, content_text, re.IGNORECASE)
                    for email in emails:
                        clean_email = email.replace('[at]', '@').replace('(at)', '@').replace('[dot]', '.').replace('(dot)', '.').replace(' ', '')
                        if clean_email not in found_data["emails"] and '@' in clean_email:
                            found_data["emails"].append(clean_email)
                            found_data["detected_keywords"].append(f"Email: {clean_email}")
                
                # Bio
                lines = content_text.split('\n')
                bio_lines = []
                bio_indicators = ['about me', 'bio', 'whoami', 'about', 'i am', "i'm"]
                for i, line in enumerate(lines):
                    line_lower = line.lower()
                    if any(indicator in line_lower for indicator in bio_indicators):
                        bio_lines.append(line.strip())
                        for j in range(1, 4):
                            if i + j < len(lines) and lines[i + j].strip() and not lines[i + j].startswith('#'):
                                bio_lines.append(lines[i + j].strip())
                            else:
                                break
                        break
                
                if bio_lines:
                    found_data["profile_bio"] = ' '.join(bio_lines)
                
                if self.target_company.lower() in content_text.lower():
                    found_data["detected_keywords"].append(f"Target '{self.target_company}' mentioned in README")
                
        except Exception as e:
            self._log(f"⚠️ Error in deep_scan_profile_text for {username}: {e}")
        
        found_data["social_links"] = list(dict.fromkeys(found_data["social_links"])) 
        found_data["detected_keywords"] = list(dict.fromkeys(found_data["detected_keywords"]))
        found_data["emails"] = list(dict.fromkeys(found_data["emails"]))
        
        return found_data

    def _is_social_media_url(self, url):
        social_domains = [
            'linkedin.com', 'twitter.com', 'x.com', 'facebook.com', 'instagram.com',
            'tiktok.com', 'youtube.com', 'github.com', 'gitlab.com', 'bitbucket.org',
            'medium.com', 'dev.to', 'stackoverflow.com', 'reddit.com', 'twitch.tv',
            'discord.com', 'discord.gg', 'slack.com', 'telegram.org', 'whatsapp.com',
            'snapchat.com', 'pinterest.com', 'tumblr.com', 'flickr.com', 'behance.net',
            'dribbble.com', 'angel.co', 'producthunt.com', 'hackernews.com'
        ]
        url_lower = url.lower()
        return any(domain in url_lower for domain in social_domains)

    def _extract_social_handles(self, text):
        handles = {
            'twitter': None,
            'github': None,
            'linkedin': None,
            'instagram': None,
            'tiktok': None
        }
        
        at_mentions = re.findall(r'@(\w+)', text)
        
        for mention in at_mentions:
            mention_lower = mention.lower()
            lines = text.lower().split('\n')
            for line in lines:
                if mention_lower in line:
                    if 'twitter' in line or 'x.com' in line:
                        handles['twitter'] = mention
                    elif 'linkedin' in line:
                        handles['linkedin'] = mention
                    elif 'github' in line:
                        handles['github'] = mention
                    elif 'instagram' in line:
                        handles['instagram'] = mention
                    elif 'tiktok' in line:
                        handles['tiktok'] = mention
        
        return handles

    def _construct_social_url(self, platform, handle):
        platform_urls = {
            'twitter': f'https://twitter.com/{handle}',
            'github': f'https://github.com/{handle}',
            'linkedin': f'https://linkedin.com/in/{handle}',
            'instagram': f'https://instagram.com/{handle}',
            'tiktok': f'https://tiktok.com/@{handle}'
        }
        return platform_urls.get(platform)

    def extract_keywords_from_results(self, repos_df: pd.DataFrame, users_df: pd.DataFrame) -> Set[str]:
        keywords = set()
        common = {'the','and','for','with','from','are','has','was','been','have','that','this',
                  'which','can','not','all','one','two','you','your','our','will','more','use',
                  'using','used','git','github','code','https','com','org','www','http','html',
                  'page','user','users','repo','repos','repository','languages'}
        
        if not repos_df.empty:
            if 'description' in repos_df.columns:
                for desc in repos_df['description'].dropna():
                    keywords.update(re.findall(r'\b[a-zA-Z]{3,}\b', str(desc).lower()))
                    keywords.update(re.findall(r'#(\w+)', str(desc)))
            
            if 'repo_name' in repos_df.columns:
                for name in repos_df['repo_name'].dropna():
                    if '/' in (n := str(name).lower()):
                        keywords.update([p for p in n.split('/')[-1].replace('-',' ').replace('_',' ').split() if len(p) > 2])
        
        if not users_df.empty:
            for col in ['Bio', 'Company_Field']:
                if col in users_df.columns:
                    for text in users_df[col].dropna():
                        keywords.update(re.findall(r'\b[a-zA-Z]{3,}\b', str(text).lower()))
        
        return keywords - common

    def add_entities_from_osint(self, entities: List[Dict[str, str]]):
        for entity in entities:
            name, username = entity.get('name', ''), entity.get('username', '')
            if name and name not in [e.get('name') for e in self.found_entities]:
                self.found_entities.append({"name": name, "username": username or name})
                self._log(f"📥 OSINT: {name} ({username or 'kein UN'}) hinzugefügt")
                if username:
                    self.searched_terms.add(username.lower())
                if name and len(name.split()) >= 2:
                    self.searched_terms.update([p for p in name.lower().split() if len(p) > 2])

    def scan_repositories(self):
        try:
            res = self._api_request(f"{self.base_url}/orgs/{self.target_company}/repos", timeout=5)
            if res and res.status_code == 200:
                repos = [{
                    "repo_name": r['full_name'], "display_name": r['name'], "url": r['html_url'],
                    "description": r.get('description', ''), "last_update": r.get('updated_at', ''),
                    "risk_score": 10, "source": "official_org"
                } for r in res.json()]
                self._log(f"✓ {len(repos)} Repos in offizieller Organisation")
                
                web = self.search_repositories_web(self.target_company)
                return pd.concat([pd.DataFrame(repos), web], ignore_index=True).drop_duplicates(subset=['url']) if not web.empty else pd.DataFrame(repos)
            
            self._log(f"Organisation nicht gefunden. Suche...")
            res = self._api_request(f"{self.base_url}/search/repositories?q={self.target_company}&per_page=30", timeout=5)
            
            if res and res.status_code == 200:
                repos = [{
                    "repo_name": i['full_name'], "display_name": i['name'], "url": i['html_url'],
                    "description": i.get('description', ''), "last_update": i.get('updated_at', ''),
                    "risk_score": 30, "source": "api_repo_search"
                } for i in res.json().get('items', [])]
                self._log(f"✓ API: {len(repos)} Repos")
                
                web = self.search_repositories_web(self.target_company)
                return pd.concat([pd.DataFrame(repos), web], ignore_index=True).drop_duplicates(subset=['url']) if not web.empty else pd.DataFrame(repos)
            
            return self.search_repositories_web(self.target_company)
        except Exception as e:
            self._log(f"❌ Fehler: {e}")
            return self.search_repositories_web(self.target_company)

    def _search_for_people(self, term: str):
        # ALways do web user search first 
        self._log(f"     🌐 Web user search for '{term}'...")
        web_users = self.search_github_web(term, "users")
        if web_users:
            web_df = pd.DataFrame(web_users)
            self.all_users = self._merge_results(web_df, self.all_users, 'Username')
            self._log(f"        ✓ Found {len(web_users)} users via web")
        
        # Then try API if available
        if not self.using_fallback and self._check_rate_limit_before_request():
            self._log(f"     🔑 API user search for '{term}'...")
            users_df = self.search_users(keyword=term, max_results=20)
            
            if not users_df.empty and self._check_rate_limit_before_request(estimated_cost=3):
                # Verify each user 
                for username in users_df['Username'].head(3):
                    if self._check_rate_limit_before_request():
                        user_info = self.verify_user_identity(username)
        
        # Also search for repos that might have employee info 
        self._log(f"     🌐 Web repo search for '{term}'...")
        repos_df = self.search_repositories_web(term)
        if not repos_df.empty:
            self.all_repos = self._merge_results(repos_df, self.all_repos, 'repo_name')
            self._log(f"        ✓ Found {len(repos_df)} repos")
            
            # Look at contributors from interesting repos 
            for _, repo in repos_df.head(2).iterrows():
                repo_name = repo.get('repo_name')
                if repo_name:
                    if not self.using_fallback and self._check_rate_limit_before_request():
                        contributors = self.get_contributors(repo_name, max_contributors=5)
                    else:
                        # Web fallback for contributors
                        self._log(f"     🌐 Web contributor search for {repo_name}...")
                        try:
                            res = self._web_request(f"https://github.com/{repo_name}")
                            if res and res.status_code == 200:
                                soup = BeautifulSoup(res.text, 'html.parser')
                                for link in soup.select('a[data-hovercard-type="user"]')[:5]:
                                    if href := link.get('href', ''):
                                        username = href[1:] if href.startswith('/') else href
                                        if username not in self.all_users.get('Username', []).values:
                                            # Try to get real name by scraping
                                            real_name = username
                                            try:
                                                scraped_data = self.html_scraper.scrape(f"https://github.com/{username}")
                                                if scraped_data and scraped_data.get('real_name'):
                                                    scraped_name = scraped_data['real_name']
                                                    if scraped_name and scraped_name != username and len(scraped_name) > 2:
                                                        real_name = scraped_name
                                            except Exception:
                                                pass
                                            
                                            contributor_info = {
                                                'Username': username,
                                                'Real_Name': real_name,
                                                'Source_Repo': repo_name,
                                                'source': 'web_contributor',
                                                'Is_Verified_Employee': 'unknown'
                                            }
                                            self.all_users = pd.concat(
                                                [self.all_users, pd.DataFrame([contributor_info])], 
                                                ignore_index=True
                                            )
                        except Exception as e:
                            self._log(f"        ⚠️ Web contributor search failed: {e}")

    def _search_by_entity(self, entity: Dict):
        name = entity.get('name', '')
        username = entity.get('username', '')
        
        self._log(f"  👤 Investigating: {name} (@{username})")
        
        # Search by real name
        if name and self._check_rate_limit_before_request():
            name_results = self.search_users_by_name(name, max_results=5)
        
        # If we have username, deep scan their profile
        if username and self._check_rate_limit_before_request():
            # Get full profile 
            user_info = self.verify_user_identity(username)
            
            # Check their repos for more people
            if self._check_rate_limit_before_request():
                repos_url = f"{self.base_url}/users/{username}/repos?per_page=5"
                response = self._api_request(repos_url)
                
                if response and response.status_code == 200:
                    for repo in response.json()[:2]:  # Check top 2 repos
                        if self._check_rate_limit_before_request():
                            self.get_contributors(repo['full_name'], max_contributors=3)

    def _explore_keywords(self, search_queue, max_keywords=3):
        # Extract keywords from what we've found
        new_keywords = self.extract_keywords_from_results(self.all_repos, self.all_users)
        self.found_keywords.update(new_keywords)
        
        # Find untested keywords
        untested = self.found_keywords - self.searched_terms
        if untested:
            # Take only most promising keywords
            promising = [k for k in untested if len(k) > 3][:max_keywords]
            
            for keyword in promising:
                if not self._check_rate_limit_before_request():
                    break
                
                self._log(f"  🔑 Testing keyword: '{keyword}'")
                
                # Quick user search for this keyword
                users_df = self.search_users(keyword=keyword, max_results=10)
                
                # Quick repo search (web, free)
                repos_df = self.search_repositories_web(keyword)
                
                self.searched_terms.add(keyword)
                if keyword not in search_queue:
                    search_queue.append(keyword)

    def iterative_search(self, external_entities: List[Dict] = None) -> ScanResult:
        self.start_time = datetime.now()
        self.api_calls_used = 0
        
        self._log(f"\n{'='*70}")
        self._log(f"🚀 COMPANY/EMPLOYEE GITHUB SCAN")
        self._log(f"{'='*70}")
        self._log(f"Target: {self.target_company}")
        self._log(f"Max API Calls: {self.max_api_calls}")
        self._log(f"Mode: {'🌐 Web Fallback' if self.using_fallback else '🔑 API Mode'}")
        
        # Add external entities from OSINT
        if external_entities:
            self._log(f"📥 Using {len(external_entities)} OSINT entities")
            self.add_entities_from_osint(external_entities)
        
        # Start with company name
        self.searched_terms.add(self.target_company.lower())
        search_queue = [self.target_company]
        
        # PHASE 1: Always do web search for the company name
        self._log(f"\n🌐 PHASE 1: Web search for '{self.target_company}'...")
        web_repos = self.search_repositories_web(self.target_company)
        if not web_repos.empty:
            self.all_repos = self._merge_results(web_repos, self.all_repos, 'repo_name')
            self._log(f"   ✓ Found {len(web_repos)} repos via web search")
        
        web_users = self.search_github_web(self.target_company, "users")
        if web_users:
            web_users_df = pd.DataFrame(web_users)
            self.all_users = self._merge_results(web_users_df, self.all_users, 'Username')
            self._log(f"   ✓ Found {len(web_users)} users via web search")
        
        # PHASE 2: If we have API calls, do deeper searches
        if not self.using_fallback and self.api_calls_used < self.max_api_calls:
            self._log(f"\n👥 PHASE 2: API search for employees...")
            self._search_for_people(self.target_company)
        
        # PHASE 3: Search by found entities 
        if self.found_entities and not self._check_timeout():
            self._log(f"\n🔍 PHASE 3: Deep diving on {min(3, len(self.found_entities))} entities...")
            for entity in self.found_entities[:10]:  
                if self._check_timeout():
                    break
                self._search_by_entity_hybrid(entity)  
        
        # PHASE 4: Keyword expansion 
        if search_queue and not self._check_timeout():
            self._log(f"\n🔑 PHASE 4: Exploring keywords...")
            self._explore_keywords_hybrid(search_queue, max_keywords=3)  
        
        self._log(f"\n{'='*70}")
        self._log(f"🎯 SCAN COMPLETE")
        self._log(f"{'='*70}")
        self._log(f"API Calls Used: {self.api_calls_used}/{self.max_api_calls}")
        self._log(f"Time: {(datetime.now() - self.start_time).total_seconds()/60:.1f} minutes")
        self._log(f"Repos: {len(self.all_repos)} | Users: {len(self.all_users)}")
        self._log(f"Keywords: {len(self.found_keywords)} | Entities: {len(self.found_entities)}")
        
        return ScanResult(
            self.all_repos, 
            self.all_users, 
            self.found_keywords, 
            self.found_entities, 
            self.searched_terms, 
            datetime.now().isoformat()
        )

    def _search_by_entity_hybrid(self, entity: Dict):
        name = entity.get('name', '')
        username = entity.get('username', '')
        
        self._log(f"  👤 Investigating: {name} (@{username})")
        
        if name:
            self._log(f"     🌐 Web search for '{name}'...")
            web_results = self.search_github_users_web(name)
            if web_results:
                web_df = pd.DataFrame(web_results)
                self.all_users = self._merge_results(web_df, self.all_users, 'Username')
                self._log(f"        ✓ Found {len(web_results)} via web")
        
        if username and not self.using_fallback and self._check_rate_limit_before_request():
            self._log(f"     🔑 API deep scan for @{username}...")
            user_info = self.verify_user_identity(username)
            
            # Check their repos for more people 
            if self._check_rate_limit_before_request():
                repos_url = f"{self.base_url}/users/{username}/repos?per_page=5"
                response = self._api_request(repos_url)
                
                if response and response.status_code == 200:
                    for repo in response.json()[:2]:
                        if self._check_rate_limit_before_request():
                            self.get_contributors(repo['full_name'], max_contributors=3)
        
        elif username:
            self._log(f"     🌐 Web search for repos by @{username}...")
            web_repos = self.search_repositories_web(f"user:{username}")
            if not web_repos.empty:
                self.all_repos = self._merge_results(web_repos, self.all_repos, 'repo_name')
                self._log(f"        ✓ Found {len(web_repos)} repos")

    def _explore_keywords_hybrid(self, search_queue, max_keywords=3):
        # Extract keywords from what we've found
        new_keywords = self.extract_keywords_from_results(self.all_repos, self.all_users)
        self.found_keywords.update(new_keywords)
        
        # Find untested keywords
        untested = self.found_keywords - self.searched_terms
        if untested:
            # Take only most promising keywords
            promising = [k for k in untested if len(k) > 3][:max_keywords]
            
            for keyword in promising:
                self._log(f"  🔑 Testing keyword: '{keyword}'")
                
                # ALWAYS do web search first 
                self._log(f"     🌐 Web search for '{keyword}'...")
                web_repos = self.search_repositories_web(keyword)
                if not web_repos.empty:
                    self.all_repos = self._merge_results(web_repos, self.all_repos, 'repo_name')
                    self._log(f"        ✓ Found {len(web_repos)} repos")
                
                web_users = self.search_github_web(keyword, "users")
                if web_users:
                    web_df = pd.DataFrame(web_users)
                    self.all_users = self._merge_results(web_df, self.all_users, 'Username')
                    self._log(f"        ✓ Found {len(web_users)} users")
                
                # If API available, do deeper search
                if not self.using_fallback and self._check_rate_limit_before_request():
                    self._log(f"     🔑 API search for '{keyword}'...")
                    users_df = self.search_users(keyword=keyword, max_results=10)
                
                self.searched_terms.add(keyword)
                if keyword not in search_queue:
                    search_queue.append(keyword)