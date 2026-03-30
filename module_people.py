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

    def _log(self, message: str):
        print(message)


    def search_duckduckgo(self, limit=10, query=None):
        # Two modes:
        #  • query=None  → company LinkedIn/XING search (original behaviour)
        #  • query="..." → arbitrary search string (used by pivot & _enrich_and_pivot)
        results = []
        try:
            with DDGS() as ddgs:
                if query:
                    ddgs_gen = ddgs.text(query, max_results=limit)
                    for r in ddgs_gen:
                        results.append({
                            "Name": self._extract_name_from_title(r.get("title", "")),
                            "URL": r.get("href", ""),
                            "Snippet": r.get("body", ""),
                            "Engine": "DuckDuckGo",
                            "Source": "Pivot",
                            "Link": r.get("href", ""),   # keep both keys for callers
                        })
                else:
                    linkedin_query = f'site:linkedin.com/in/ "{self.company}"'
                    ddgs_gen = ddgs.text(linkedin_query, max_results=limit)
                    for r in ddgs_gen:
                        results.append({
                            "Name": self._extract_name_from_title(r.get("title", "")),
                            "URL": r.get("href", ""),
                            "Snippet": r.get("body", ""),
                            "Engine": "DuckDuckGo",
                            "Source": "LinkedIn",
                        })

                    # Only top-up with XING if we still need more results
                    remaining = limit - len(results)
                    if remaining > 0:
                        xing_query = f'site:xing.com/profile/ "{self.company}"'
                        xing_gen = ddgs.text(xing_query, max_results=remaining)
                        for x in xing_gen:
                            results.append({
                                "Name": self._extract_name_from_title(x.get("title", "")),
                                "URL": x.get("href", ""),
                                "Snippet": x.get("body", ""),
                                "Engine": "DuckDuckGo",
                                "Source": "XING",
                            })
        except Exception as e:
            print(f"  ⚠️ DuckDuckGo Error: {e}")

        return results
    
    def perform_pivot_scans(self, discovered_people, limit_per_pivot=3):
        # For each person, run two DuckDuckGo queries:
        #  1. "Real Name Company"
        #  2. "Username Company"

        pivot_results = []
        seen_queries = set()

        for person in discovered_people:
            name = person.get("Name")
            username = person.get("Username")

            if name and name != "Unknown":
                query = f'{name} {self.company}'
                if query not in seen_queries:
                    print(f"🔄 Pivoting on name: {query}")
                    hits = self.search_duckduckgo(query=query, limit=limit_per_pivot)
                    pivot_results.extend(hits)
                    seen_queries.add(query)

            if username and username not in ("Unknown", "N/A", ""):
                query = f'{username} {self.company}'
                if query not in seen_queries:
                    print(f"🔄 Pivoting on username: {query}")
                    hits = self.search_duckduckgo(query=query, limit=limit_per_pivot)
                    pivot_results.extend(hits)
                    seen_queries.add(query)

        return pivot_results
    
    def _request_with_backoff(self, url, params=None):
        max_retries = 3
        backoff_factor = 2

        for attempt in range(max_retries):
            try:
                now = time.time()
                elapsed = now - self.last_request
                if elapsed < self.min_delay:
                    time.sleep(self.min_delay - elapsed)

                response = self.session.get(
                    url,
                    params=params,
                    headers=self._get_headers(),
                    timeout=15,          # ← always 15 s; was incorrectly passed as kwarg
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
        print("🔍 Searching via DuckDuckGo...")
        ddg_res = self.search_duckduckgo(limit)
        all_results.extend(ddg_res)
        print(f"  ✓ DuckDuckGo: {len(ddg_res)} results")

        # 2. Bing 
        if len(all_results) < limit:
            print("🔍 Searching via Bing...")
            bing_res = self.search_bing(limit - len(all_results))
            all_results.extend(bing_res)
            print(f"  ✓ Bing: {len(bing_res)} results")

        # 3. Startpage 
        if len(all_results) < limit:
            print("🔍 Searching via Startpage...")
            startpage_res = self.search_startpage(limit - len(all_results))
            all_results.extend(startpage_res)
            print(f"  ✓ Startpage: {len(startpage_res)} results")

        # 4. Yahoo
        if len(all_results) < limit:
            print("🔍 Searching via Yahoo...")
            yahoo_res = self.search_yahoo(limit - len(all_results))
            all_results.extend(yahoo_res)
            print(f"  ✓ Yahoo: {len(yahoo_res)} results")

        # 5. Google Dorking 
        if len(all_results) < 10:
            print("🔍 Searching via Google Dorking (slow)...")
            google_res = self.search_google_dork(5)
            all_results.extend(google_res)
            print(f"  ✓ Google Dork: {len(google_res)} results")

        if not all_results:
            return pd.DataFrame()
        
        # Deduplicate and clean
        df = pd.DataFrame(all_results).drop_duplicates(subset=["URL"])
        self.discovered_people = []

        # Extract usernames from LinkedIn URLs
        if 'URL' in df.columns:
            df['Username'] = df['URL'].apply(self._extract_linkedin_username)
        
        # Clean names
        if 'Name' in df.columns:
            df['Name'] = df['Name'].apply(self._clean_name)
        
        print(f"\n✅ Total: {len(df)} unique people found")
        
        print("🔍 Scraping profile pages for additional information...")
        
        enriched_data = []
        for idx, row in df.iterrows():
            url = row.get("URL")
            username_fallback = row.get("Name", "Unknown")

            if url and pd.notna(url):
                should_scrape = True
                if hasattr(self, "scraped_urls_global") and url in self.scraped_urls_global:
                    should_scrape = False

                if should_scrape:
                    if hasattr(self, "scraped_urls_global"):
                        self.scraped_urls_global.add(url)
                    scraped = self.html_scraper.scrape(url, target_company=self.company)
                else:
                    scraped = {
                        "real_name": None,
                        "emails": [],
                        "social_links": [],
                        "company": None,
                        "people_found": [],
                    }
                if scraped and scraped.get("real_name"):
                    row["Real_Name"] = scraped["real_name"]
                    row["Name"] = scraped["real_name"]
                    row["Username"] = username_fallback
                else:
                    # Try to derive a display name from the URL slug
                    derived = self._slug_to_name(url)
                    row["Real_Name"] = derived if derived != "Unknown" else None
                    row["Name"] = derived
                    row["Username"] = username_fallback

                row["Emails"] = scraped.get("emails", []) if scraped else []
                row["Found_Links"] = list(
                    set([url] + (scraped.get("social_links", []) if scraped else []))
                )

                if scraped and scraped.get("company"):
                    row["Offizielle_Firma"] = scraped["company"]

                if scraped and scraped.get("people_found"):
                    for person in scraped["people_found"]:
                        if person not in self.discovered_people:
                            self.discovered_people.append(person)
                        if person.get("url") and person["url"] not in row["Found_Links"]:
                            row["Found_Links"].append(person["url"])
            else:
                row["Username"] = username_fallback
                row["Name"] = username_fallback

            enriched_data.append(row)

        df_enriched = pd.DataFrame(enriched_data)

        if self.discovered_people:
            print(f"\n✅ Discovered {len(self.discovered_people)} additional people from pages")

        for col in [
            "Name", "URL", "Snippet", "Engine", "Source", "Username",
            "Scraped_Name", "Scraped_Social_Links",
        ]:
            if col not in df_enriched.columns:
                df_enriched[col] = None

        real_names_found = df_enriched["Name"].apply(
            lambda n: bool(n) and n != "Unknown" and " " in str(n)
        ).sum()
        print(
            f"\n✅ Enriched data: {len(df_enriched)} profiles "
            f"with {real_names_found} real names found"
        )

        return df_enriched

    def _slug_to_name(self, url: str) -> str:
        if not url or not isinstance(url, str):
            return "Unknown"

        # Extract slug from LinkedIn or XING URL
        slug = None
        for pattern in [
            r"linkedin\.com/in/([^/?#&]+)",
            r"xing\.com/profile/([^/?#&]+)",
        ]:
            m = re.search(pattern, url, re.IGNORECASE)
            if m:
                slug = m.group(1)
                break

        if not slug:
            return "Unknown"

        # URL-decode (e.g. %C3%BC → ü)
        try:
            from urllib.parse import unquote
            slug = unquote(slug)
        except Exception:
            pass

        # Strip trailing tracking / query params LinkedIn sometimes leaves
        slug = slug.split("?")[0].rstrip("/")

        # Strip trailing numeric hash (e.g. -a82889101, -1a2253126, -219389228)
        slug = re.sub(r"-[a-f0-9]{6,}$", "", slug, flags=re.IGNORECASE)
        slug = re.sub(r"-\d{5,}$", "", slug)

        # Split on hyphens first
        parts = [p for p in slug.split("-") if p]

        if len(parts) == 1:
            # Single run-together slug — no hyphens 
            # We can't reliably split arbitrary German compound names,
            # so just title-case the whole thing as a single token.
            word = parts[0]
            # Capitalise on lowercase→uppercase transitions (camelCase guard)
            display = re.sub(r"([a-z])([A-Z])", r"\1 \2", word).title()
            return display if len(display) > 2 else "Unknown"

        # Multi-part slug: remove company keyword parts and pure-numeric parts
        company_lower = self.company.lower()
        filtered = []
        for part in parts:
            part_lower = part.lower()
            # Drop the company name itself if it snuck into the slug
            if part_lower == company_lower:
                continue
            # Drop pure numbers
            if re.fullmatch(r"\d+", part):
                continue
            # Drop very short connector tokens that aren't initials
            if len(part) == 1 and not part.isupper():
                continue
            filtered.append(part.capitalize())

        if not filtered:
            return "Unknown"

        # LinkedIn often appends the job title or employer after the actual name;
        # heuristically keep only the first 2–3 tokens as the "name" portion
        # unless they're all very short (initials + surname).
        name_tokens = filtered[:3] if len(filtered) > 3 else filtered
        return " ".join(name_tokens)

    def search_bing(self, limit):
        results = []
        query = f'site:linkedin.com/in/ "{self.company}"'
        
        params = {
            'q': query,
            'count': min(limit, 50),
            'first': 1
        }
        
        try:
            response = self._request_with_backoff("https://www.bing.com/search", params=params)
            
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
                            "Engine": "Bing",
                            "Source": "LinkedIn"
                        })
        except Exception as e:
            print(f"  ⚠️ Bing error: {e}")
        
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
                            "Engine": "Startpage",
                            "Source": "LinkedIn"
                        })
        except Exception as e:
            print(f"  ⚠️ Startpage error: {e}")
        
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
                            "Engine": "Yahoo",
                            "Source": "LinkedIn"
                        })
        except Exception as e:
            print(f"  ⚠️ Yahoo error: {e}")
        
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
                                "Engine": "Google Dork",
                                "Source": "LinkedIn"
                            })
                    
                    time.sleep(2)  # Be gentle with Google
        except Exception as e:
            print(f"  ⚠️ Google Dorking error: {e}")
        
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