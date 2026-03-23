import time
import random
import requests
from googlesearch import search
from ddgs import DDGS
import pandas as pd
import re
from bs4 import BeautifulSoup
from html_scraper import HTMLScraper

class PeopleScanner:
    def __init__(self, company_name):
        self.company = company_name
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/617.1",
            "Mozilla/5.0 (X11; Linux x86_64) Firefox/121.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
        ]
        self.session = requests.Session()
        self.last_request = 0
        self.min_delay = 3  # Increased slightly for safety
        self.html_scraper = HTMLScraper(delay=1.5)
        self.discovered_people = []
    # TODO Not usefull atm wrong calls? Wrong setup?
    def perform_pivot_scans(self, discovered_people, limit_per_pivot=3):
        pivot_results = []
        # Process only unique entities to save time
        seen_queries = set()

        for person in discovered_people:
            name = person.get('Name')
            username = person.get('Username')
            
            # Pattern 1: "Real Name" "Company"
            if name and name != "Unknown":
                query = f'"{name}" "{self.company}"'
                if query not in seen_queries:
                    print(f"🔄 Pivoting: {query}")
                    pivot_results.extend(self.search_duckduckgo(query=query, limit=limit_per_pivot))
                    seen_queries.add(query)

            # Pattern 2: "Username" "Company"
            if username and username != "Unknown":
                query = f'"{username}" "{self.company}"'
                if query not in seen_queries:
                    print(f"🔄 Pivoting: {query}")
                    pivot_results.extend(self.search_duckduckgo(query=query, limit=limit_per_pivot))
                    seen_queries.add(query)
        
        return pivot_results
    
    def _request_with_backoff(self, url, params=None):
        max_retries = 3
        backoff_factor = 2
        
        for attempt in range(max_retries):
            try:
                # Rate limit before every request
                now = time.time()
                if now - self.last_request < self.min_delay:
                    time.sleep(self.min_delay - (now - self.last_request))
                
                response = self.session.get(
                    url, 
                    params=params, 
                    headers=self._get_headers(), 
                    timeout=15
                )
                self.last_request = time.time()

                if response.status_code == 429:
                    wait = backoff_factor ** (attempt + 1) + random.uniform(0, 1)
                    print(f"  ⚠️ Rate Limit (429) on {url}. Waiting {wait:.1f}s...")
                    time.sleep(wait)
                    continue
                
                return response
            except Exception as e:
                print(f"  ❌ Request error: {e}")
                return None
        return None

    def _get_headers(self):
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

    def scan_all_sources(self, limit=10):
        all_results = []
        
        # 1. DuckDuckGo
        print("🔍 Suche via DuckDuckGo...")
        ddg_res = self.search_duckduckgo(limit)
        all_results.extend(ddg_res)
        print(f"  ✓ DuckDuckGo: {len(ddg_res)} Ergebnisse")

        # 2. Bing 
        if len(all_results) < limit:
            print("🔍 Suche via Bing...")
            bing_res = self.search_bing(limit - len(all_results))
            all_results.extend(bing_res)
            print(f"  ✓ Bing: {len(bing_res)} Ergebnisse")

        # 3. Startpage 
        if len(all_results) < limit:
            print("🔍 Suche via Startpage...")
            startpage_res = self.search_startpage(limit - len(all_results))
            all_results.extend(startpage_res)
            print(f"  ✓ Startpage: {len(startpage_res)} Ergebnisse")

        # 4. Yahoo
        if len(all_results) < limit:
            print("🔍 Suche via Yahoo...")
            yahoo_res = self.search_yahoo(limit - len(all_results))
            all_results.extend(yahoo_res)
            print(f"  ✓ Yahoo: {len(yahoo_res)} Ergebnisse")

        # 5. Google Dorking 
        if len(all_results) < 10:
            print("🔍 Suche via Google Dorking (langsam)...")
            google_res = self.search_google_dork(5)
            all_results.extend(google_res)
            print(f"  ✓ Google Dork: {len(google_res)} Ergebnisse")

        if not all_results:
            return pd.DataFrame()
        
        # Deduplicate and clean
        df = pd.DataFrame(all_results)
        df = df.drop_duplicates(subset=['URL'])
        self.discovered_people = []

        # Extract usernames from LinkedIn URLs
        if 'URL' in df.columns:
            df['Username'] = df['URL'].apply(self._extract_linkedin_username)
        
        # Clean names
        if 'Name' in df.columns:
            df['Name'] = df['Name'].apply(self._clean_name)
        
        print(f"\n✅ Gesamt: {len(df)} einzigartige Personen gefunden")
        
        print("🔍 Scraping profile pages for additional information...")
        
        enriched_data = []
        
        for idx, row in df.iterrows():
            url = row.get('URL')
            # Preserve the username/original name as a backup
            username_fallback = row.get('Name', 'Unknown') 
            
            if url and pd.notna(url):
                # Check global cache before scraping
                should_scrape = True
                if hasattr(self, 'scraped_urls_global') and url in self.scraped_urls_global:
                    self._log(f"  ⏭️ Skipping {url} (already scraped globally)")
                    should_scrape = False
                
                if should_scrape:
                    # Add to global cache
                    if hasattr(self, 'scraped_urls_global'):
                        self.scraped_urls_global.add(url)
                    
                    # Perform the scrape
                    scraped = self.html_scraper.scrape(url, target_company=self.company)
                else:
                    # Create a minimal scraped dict with just the URL
                    scraped = {'real_name': None, 'emails': [], 'social_links': [], 'company': None, 'people_found': []}
                
                # If we found a real name, use it as the primary identity
                if scraped.get('real_name'):
                    # Check if it's actually different from the username
                    found_name = scraped['real_name']
                    row['Real_Name'] = found_name
                    row['Name'] = found_name # This updates the display name
                    row['Username'] = username_fallback # Keep the old name as the username handle
                else:
                    row['Real_Name'] = None
                    row['Username'] = username_fallback

                row['Emails'] = scraped.get('emails', [])
                row['Gefundene_Links'] = list(set([url] + scraped.get('social_links', [])))

                # Handle Company
                if scraped.get('company'):
                    row['Offizielle_Firma'] = scraped['company']
                
                # Store newly discovered people for secondary scanning
                if scraped.get('people_found'):
                    for person in scraped['people_found']:
                        # Add to discovered people for later processing
                        if person not in self.discovered_people:
                            self.discovered_people.append(person)
                        
                        # Also add their URL to current person's links if not already there
                        if person.get('url') and person['url'] not in row['Gefundene_Links']:
                            row['Gefundene_Links'].append(person['url'])
                            self._log(f"Added discovered person's URL to links: {person['url']}")
            else:
                # Even if there's no URL or it's invalid, keep a placeholder
                row['Username'] = username_fallback
                row['Name'] = username_fallback
            
            enriched_data.append(row)
        
        # Create final DataFrame
        df_enriched = pd.DataFrame(enriched_data)
        
        # Add discovered people to the result (they'll be processed separately)
        if self.discovered_people:
            print(f"\n✅ Discovered {len(self.discovered_people)} additional people from pages")
        
        # Ensure all expected columns exist
        for col in ['Name', 'URL', 'Snippet', 'Quelle', 'Source', 'Username', 'Scraped_Name', 'Scraped_Social_Links']:
            if col not in df_enriched.columns:
                df_enriched[col] = None
        
        print(f"\n✅ Enriched data: {len(df_enriched)} profiles with {df_enriched['Scraped_Name'].notna().sum()} real names found")
        
        return df_enriched

    def search_duckduckgo(self, limit):
        results = []
        try:
            with DDGS() as ddgs:
                # Search for LinkedIn profiles
                ddgs_gen = ddgs.text(
                    f'site:linkedin.com/in/ "{self.company}"', 
                    max_results=limit
                )
                for r in ddgs_gen:
                    title = r.get('title', '')
                    name = self._extract_name_from_title(title)
                    
                    results.append({
                        "Name": name,
                        "URL": r.get('href', ''),
                        "Snippet": r.get('body', ''),
                        "Quelle": "DuckDuckGo",
                        "Source": "LinkedIn"
                    })
                    
                    # Also search for XING profiles 
                    if len(results) < limit:
                        xing_gen = ddgs.text(
                            f'site:xing.com/profile/ "{self.company}"',
                            max_results=limit - len(results)
                        )
                        for x in xing_gen:
                            results.append({
                                "Name": self._extract_name_from_title(x.get('title', '')),
                                "URL": x.get('href', ''),
                                "Snippet": x.get('body', ''),
                                "Quelle": "DuckDuckGo",
                                "Source": "XING"
                            })
        except Exception as e:
            print(f"  ⚠️ DuckDuckGo Fehler: {e}")
        
        return results

    def search_bing(self, limit):
        results = []
        query = f'site:linkedin.com/in/ "{self.company}"'
        
        params = {
            'q': query,
            'count': min(limit, 50),
            'first': 1
        }
        
        try:
            response = self._request_with_backoff(
                'https://www.bing.com/search',
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Bing results are in <li class="b_algo">
                for result in soup.select('li.b_algo')[:limit]:
                    title_elem = result.select_one('h2 a')
                    if not title_elem:
                        continue
                    
                    url = title_elem.get('href', '')
                    title = title_elem.text
                    snippet_elem = result.select_one('p')
                    snippet = snippet_elem.text if snippet_elem else ''
                    
                    if 'linkedin.com/in/' in url:
                        results.append({
                            "Name": self._extract_name_from_title(title),
                            "URL": url,
                            "Snippet": snippet,
                            "Quelle": "Bing",
                            "Source": "LinkedIn"
                        })
        except Exception as e:
            print(f"  ⚠️ Bing Fehler: {e}")
        
        return results

    def search_startpage(self, limit):
        results = []
        query = f'site:linkedin.com/in/ "{self.company}"'
        
        params = {
            'q': query,
            'num': min(limit, 10)
        }
        
        try:
            response = self._request_with_backoff(
                'https://www.startpage.com/sp/search',
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Startpage results are in <div class="w-gl__result">
                for result in soup.select('div.w-gl__result')[:limit]:
                    link_elem = result.select_one('a.result-link')
                    if not link_elem:
                        continue
                    
                    url = link_elem.get('href', '')
                    title = link_elem.text
                    
                    if 'linkedin.com/in/' in url:
                        results.append({
                            "Name": self._extract_name_from_title(title),
                            "URL": url,
                            "Snippet": "",
                            "Quelle": "Startpage",
                            "Source": "LinkedIn"
                        })
        except Exception as e:
            print(f"  ⚠️ Startpage Fehler: {e}")
        
        return results

    def search_yahoo(self, limit):
        results = []
        query = f'site:linkedin.com/in/ "{self.company}"'
        
        params = {
            'p': query,
            'output': 'rss',
            'n': min(limit, 50)
        }
        
        try:
            response = self._request_with_backoff(
                'https://search.yahoo.com/search',
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                # Parse RSS feed
                import xml.etree.ElementTree as ET
                root = ET.fromstring(response.text)
                
                # RSS items are in channel/item
                for item in root.findall('.//item')[:limit]:
                    title = item.find('title').text if item.find('title') is not None else ''
                    link = item.find('link').text if item.find('link') is not None else ''
                    description = item.find('description').text if item.find('description') is not None else ''
                    
                    if 'linkedin.com/in/' in link:
                        results.append({
                            "Name": self._extract_name_from_title(title),
                            "URL": link,
                            "Snippet": description,
                            "Quelle": "Yahoo",
                            "Source": "LinkedIn"
                        })
        except Exception as e:
            print(f"  ⚠️ Yahoo Fehler: {e}")
        
        return results

    def search_google_dork(self, limit):
        results = []
        try:
            # Use requests directly with proper headers
            query = f'site:linkedin.com/in/ "{self.company}"'
            url = f"https://www.google.com/search?q={requests.utils.quote(query)}&num={limit}"
            
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = self.session.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Google search results are in <div class="g">
                for result in soup.select('div.g')[:limit]:
                    link_elem = result.select_one('a')
                    if not link_elem:
                        continue
                    
                    href = link_elem.get('href', '')
                    # Google results have URLs in format /url?q=ACTUAL_URL&...
                    if '/url?q=' in href:
                        import urllib.parse
                        parsed = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                        actual_url = parsed.get('q', [None])[0]
                        if actual_url and 'linkedin.com/in/' in actual_url:
                            title_elem = result.select_one('h3')
                            title = title_elem.text if title_elem else ''
                            
                            results.append({
                                "Name": self._extract_name_from_title(title),
                                "URL": actual_url,
                                "Snippet": "",
                                "Quelle": "Google Dork",
                                "Source": "LinkedIn"
                            })
                    
                    time.sleep(2)  # Be gentle with Google
        except Exception as e:
            print(f"  ⚠️ Google Dorking Fehler: {e}")
        
        return results

    def _fix_encoding(self, text):
        if not text or not isinstance(text, str):
            return text
        
        # Fix common mojibake patterns
        replacements = {
            'Ã©': 'é', 'Ã¨': 'è', 'Ãª': 'ê', 'Ã«': 'ë',
            'Ã¤': 'ä', 'Ã¶': 'ö', 'Ã¼': 'ü', 'ÃŸ': 'ß',
            'Ã¡': 'á', 'Ã¢': 'â', 'Ã£': 'ã', 'Ã§': 'ç',
            'Ã±': 'ñ', 'Ã¬': 'ì', 'Ã®': 'î', 'Ã¯': 'ï',
            'Ã²': 'ò', 'Ã³': 'ó', 'Ã´': 'ô', 'Ãµ': 'õ',
            'Ã¹': 'ù', 'Ãº': 'ú', 'Ã»': 'û', 'Ã½': 'ý',
            'Ã¾': 'þ', 'Ã¿': 'ÿ',
            'Ãƒ': 'Ã', 'Â': '', 'â€“': '-', 'â€”': '-',
            'â€™': "'", 'â€˜': "'", 'â€œ': '"', 'â€': '"'
        }
        
        for wrong, correct in replacements.items():
            text = text.replace(wrong, correct)
        
        # Try to properly decode if it's bytes
        try:
            if isinstance(text, bytes):
                text = text.decode('utf-8')
            else:
                # If it's already string, try to fix encoding
                text = text.encode('latin1').decode('utf-8', errors='ignore')
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
        
        return text.strip()

    def _clean_name(self, name):
        if not name or pd.isna(name):
            return "Unknown"
        
        name = str(name)
        # Fix encoding first
        name = self._fix_encoding(name)
        
        # Remove common patterns
        name = re.sub(r'\s*[-|]\s*(LinkedIn|XING|Profile).*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*\([^)]*\)', '', name)
        name = re.sub(r'\s*\[[^\]]*\]', '', name)
        
        return name.strip() or "Unknown"

    def _extract_name_from_title(self, title):
        if not title:
            return "Unknown"
        
        # Fix encoding 
        title = self._fix_encoding(title)
        
        # Remove common patterns
        title = re.sub(r'\s*[-|]\s*(LinkedIn|Profile|XING).*$', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\s*\([^)]*\)', '', title)
        title = re.sub(r'\s*\[[^\]]*\]', '', title)
        
        return title.strip() or "Unknown"

    def _extract_name_from_url(self, url):
        username = self._extract_linkedin_username(url)
        if username:
            name_parts = username.replace('-', ' ').replace('_', ' ').split()
            return ' '.join(part.capitalize() for part in name_parts)
        return "Unknown"

    def _extract_linkedin_username(self, url):
        if not url or not isinstance(url, str):
            return None
        
        patterns = [
            r'linkedin\.com/in/([^/?#]+)',
            r'linkedin\.com/pub/([^/?#]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url.lower())
            if match:
                return match.group(1)
        
        return None