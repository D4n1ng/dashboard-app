# html_scraper.py
import html
import requests
import re
import time
import random
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import urllib.parse

class HTMLScraper:

    SOCIAL_DOMAINS = [
        'linkedin.com/in', 'twitter.com', 'x.com', 'instagram.com', 'facebook.com',
        'xing.com/profile', 'stackoverflow.com', 'github.com', 'medium.com', 'dev.to',
    ]

    # Patterns for real names (Updated for robustness)
    NAME_PATTERNS = [
        # GitHub vcard (Matches your Simorenarium example perfectly)
        (r'class="[^"]*p-name vcard-fullname[^"]*"[^>]*>\s*([^<]+?)\s*</span>', 1),
        # General itemprop="name" (Schema.org standard)
        (r'itemprop="name"[^>]*>\s*([^<]+?)\s*<', 1),
        # LinkedIn patterns
        (r'<title>([^(]+)\([0-9]+\)\s*\|\s*LinkedIn</title>', 1),
        (r'<title>([^|]+)\s*\|\s*LinkedIn</title>', 1),
        (r'"firstName":"([^"]+)","lastName":"([^"]+)"', 2),
        # Meta tags
        (r'<meta[^>]*name="author"[^>]*content="([^"]+)"', 1),
        (r'<meta[^>]*property="og:title"[^>]*content="([^"]+)"', 1),
        # Headings
        (r'<h1[^>]*>\s*([^<]{5,100}?)\s*</h1>', 1),
    ]

    def __init__(self, user_agent: str = None, delay: float = 1.0, verbose: bool = True):
        self.session = requests.Session()
        self.scraped_urls_cache = set()  # Track which URLs we've already scraped
        self.session.headers.update({
            'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.delay = delay
        self.verbose = verbose

    def _log(self, message: str):
        if self.verbose:
            print(message, flush=True)

    def _clean_extracted_name(self, name: str) -> Optional[str]:
        if not name:
            return None
        # Replace newlines/tabs with spaces, then collapse multiple spaces
        clean_name = " ".join(name.split())
        # Strip common trailing separators found in titles
        clean_name = re.sub(r'\s*[|•\-].*$', '', clean_name).strip()

        # Validation: Filter out UI elements or very short/long strings
        if len(clean_name) < 3 or len(clean_name) > 60 or "/" in clean_name:
            return None
        return clean_name

    def fetch_page(self, url: str) -> Optional[str]:
        self._log(f"  🔍 Fetching: {url}")

        if 'linkedin.com' in url.lower():
            self._log(f"LinkedIn detected – skipping fetch.")
            return None

        try:
            time.sleep(self.delay + random.uniform(0.5, 1.5))
            response = self.session.get(url, timeout=10, allow_redirects=True)
            self._log(f"  📊 Status: {response.status_code}, Length: {len(response.text)} bytes")

            if response.status_code == 200:
                if 'sign in' in response.text.lower()[:1000]:
                    self._log(f"  ⚠️ Page may require login")
                return response.text
            else:
                self._log(f"  ⚠️ Failed to fetch: status {response.status_code}")
                return None
        except Exception as e:
            self._log(f"  ❌ Error fetching: {e}")
            return None

    def _extract_username_from_linkedin_url(self, url: str) -> Optional[str]:
        patterns = [r'linkedin\.com/in/([^/?#]+)', r'linkedin\.com/pub/([^/?#]+)']
        for pattern in patterns:
            match = re.search(pattern, url.lower())
            if match:
                return match.group(1)
        return None

    def is_personal_profile_url(self, url: str) -> bool:
        url_lower = url.lower()
        bad_paths = ['/company/', '/pages/', '/school/', '/login', '/signup', '/jobs', '/orgs/', '/about', '/policy']
        if any(bad in url_lower for bad in bad_paths):
            return False

        parsed_url = urllib.parse.urlparse(url)
        for domain in self.SOCIAL_DOMAINS:
            # Check if the domain (without path specifics) is in the URL and it has a path
            if domain.replace('/in', '').replace('/profile', '') in url_lower:
                if len(parsed_url.path) > 1:
                    return True
        return False

    def extract_social_links(self, html: str, current_url: str = None) -> List[str]:
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        links = []
        all_links = soup.find_all('a', href=True)
        self._log(f"    Found {len(all_links)} total links on page")

        for a in all_links:
            href = a['href']
            # Normalize relative URLs
            if href.startswith('/'):
                if current_url and 'github.com' in current_url:
                    href = 'https://github.com' + href
                else:
                    continue
            elif not href.startswith('http'):
                continue

            href_lower = href.lower()
            if current_url and current_url.lower() in href_lower:
                continue

            matched_domain = None
            for domain in self.SOCIAL_DOMAINS:
                if domain in href_lower:
                    matched_domain = domain
                    break

            if matched_domain and self.is_personal_profile_url(href):
                if href not in links:
                    links.append(href)
                    self._log(f"      ✅ Added {matched_domain}: {href[:60]}...")

        self._log(f"    Found {len(links)} personal profile links")
        return links

    def extract_emails(self, html: str) -> List[str]:
        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html)
        return list(dict.fromkeys(emails))

    def extract_company_from_text(self, html: str, target_company: str = None) -> Optional[str]:
        if target_company and target_company.lower() in html.lower():
            return target_company
        match = re.search(r'works? at ([A-Za-z0-9\s&]+)', html, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    # TODO many users dont get properly analyzed f.e. Torsten Klein appears in console prints but not in the final results and is not on the Mitarbeiter page
    # TODO some users lose their surname and then are only further processed with the first name which then leads to no results and also no further processing of found accounts cause of the skipping of already processed names - maybe we should not skip already processed names but only urls or add a check if the name is already in the results and if not do not skip it
    # specialized method for GitHub 
    def extract_github_profile_data(self, html: str, username: str) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        data = {'real_name': None, 'emails': [], 'social_links': [], 'company': None}
        self._log(f"    Performing specialized GitHub extraction for {username}...")

        # Real Name Extraction 
        name_span = soup.find('span', class_=re.compile(r'p-name vcard-fullname'))
        if name_span:
            name_text = name_span.get_text(strip=True)
            if name_text and len(name_text) > 2 and '@' not in name_text:
                data['real_name'] = name_text
                self._log(f"      ✅ Found GitHub name via p-name span: {name_text}")
                return data #Early return if we found a valid name here
        if not name_span:
            # Fallback: Find any span with a class containing vcard-fullname
            name_span = soup.find('span', class_=re.compile(r'vcard-fullname'))
        if not name_span:
            # Fallback: Find any element with itemprop="name" (common schema)
            name_span = soup.find(attrs={"itemprop": "name"})

        if name_span:
            name_text = name_span.get_text(strip=True)
            if name_text and len(name_text) > 2 and '@' not in name_text:
                data['real_name'] = name_text
                self._log(f"      ✅ Found GitHub name via span/element: {data['real_name']}")

        # Look for the h1 with vcard-names (container)
        if not data['real_name']:
            h1 = soup.find('h1', class_=re.compile(r'vcard-names'))
            if h1:
                name_text = h1.get_text(strip=True)
                if name_text and len(name_text) > 2 and '@' not in name_text:
                    data['real_name'] = name_text
                    self._log(f"      ✅ Found GitHub name via h1: {data['real_name']}")

        # Use the generic regex patterns as a last resort
        if not data['real_name']:
            self._log(f"      ⚠️ Specialized extraction failed, trying generic regex...")
            for pattern, group_idx in self.NAME_PATTERNS:
                try:
                    match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
                    if match:
                        if group_idx == 2 and match.groups() and len(match.groups()) >= 2:
                            raw_name = f"{match.group(1).strip()} {match.group(2).strip()}"
                        else:
                            raw_name = match.group(group_idx)
                        clean_name = self._clean_extracted_name(raw_name)
                        if clean_name:
                            data['real_name'] = clean_name
                            self._log(f"      ✅ Found GitHub name via regex: {clean_name}")
                            break
                except Exception:
                    continue

        if not data['real_name']:
            data['real_name'] = username
            self._log(f"      ⚠️ Using username as fallback: {username}")

        # Extract Comapny
        company_tag = soup.find('span', class_='p-org') or soup.find(attrs={"itemprop": "worksFor"})
        if company_tag and company_tag.text:
            data['company'] = company_tag.text.strip()

        # Extract Social Links
        for link in soup.select('ul.vcard-details a'):
            href = link.get('href', '')
            if href.startswith('http') and 'github.com' not in href:
                data['social_links'].append(href)

        # Extract Emails from README (if available)
        readme_article = soup.find('article', class_=re.compile(r'markdown-body'))
        if readme_article:
            text = readme_article.get_text(separator=' ')
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            data['emails'].extend(emails)

            for a in readme_article.find_all('a', href=True):
                href = a['href']
                href = re.sub(r'\s+', '', href)  # Remove all whitespace
                href = href.strip()
                if any(domain in href.lower() for domain in ['twitter.com', 'linkedin.com', 'xing.com', 'facebook.com']):
                    href = href.strip().replace(' ', '').replace('\n', '').replace('\r', '')
                    data['social_links'].append(href)

        # Fetch Raw Readme as a last resort for emails if none found in the rendered page
        if not data['emails']:
            for branch in ['main', 'master']:
                readme_url = f"https://raw.githubusercontent.com/{username}/{username}/{branch}/README.md"
                try:
                    resp = self.session.get(readme_url, timeout=5)
                    if resp.status_code == 200:
                        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', resp.text)
                        data['emails'].extend(emails)
                        break
                except Exception:
                    continue

        data['emails'] = list(set([e for e in data['emails'] if not e.endswith(('.png', '.jpg', '.svg', '.gif'))]))
        data['social_links'] = list(set(data['social_links']))
        return data

    def extract_real_name(self, html: str, url: str = None) -> Optional[str]:
        if not html:
            return None
        self._log(f"    Looking for real name via generic patterns...")

        # First, fix encoding of the entire HTML if needed
        try:
            # Try to decode as UTF-8 if it's bytes
            if isinstance(html, bytes):
                html = html.decode('utf-8', errors='ignore')
            else:
                # If it's a string, check for mojibake
                html = html.encode('latin1', errors='ignore').decode('utf-8', errors='ignore')
        except:
            pass

        for pattern, group_idx in self.NAME_PATTERNS:
            try:
                match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
                if match:
                    if group_idx == 2 and match.groups() and len(match.groups()) >= 2:
                        raw_name = f"{match.group(1).strip()} {match.group(2).strip()}"
                    else:
                        raw_name = match.group(group_idx)
                    raw_name = self._fix_encoding(raw_name)
                    clean_name = self._clean_extracted_name(raw_name)
                    if clean_name:
                        self._log(f"      ✅ Found via generic regex: {clean_name}")
                        return clean_name
            except Exception:
                continue
        return None
    
    def _fix_encoding(self, text):
        if not text or not isinstance(text, str):
            return text
        
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
        
        try:
            text = text.encode('latin1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError, AttributeError):
            pass
        
        return text.strip()

    def extract_people_from_page(self, html: str, source_url: str) -> List[Dict]:
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        people = []
        found_urls = set()  # Track URLs we've already found to avoid duplicates
        
        # Look for ALL LinkedIn profile links in the entire page
        for a in soup.find_all('a', href=True):
            href = a['href']
            
            # Normalize relative URLs
            if href.startswith('/'):
                if 'github.com' in source_url:
                    href = 'https://github.com' + href
                else:
                    # Skip other relative URLs as they're probably not LinkedIn
                    continue
            elif not href.startswith('http'):
                continue
            
            # Check if it's a LinkedIn profile URL (any variation)
            if 'linkedin.com/in/' in href.lower() or 'linkedin.com/pub/' in href.lower():
                if href in found_urls:
                    continue  # Skip duplicates
                    
                found_urls.add(href)
                
                # Try to get name from link text first
                name = None
                if a.text and len(a.text.strip()) > 3:
                    name = a.text.strip()
                    self._log(f"      📝 Found LinkedIn name from link text: {name}")
                else:
                    # If no link text, extract from URL
                    username = self._extract_username_from_linkedin_url(href)
                    if username:
                        # Clean up the username
                        clean_username = re.sub(r'[-_][a-zA-Z0-9]+$', '', username)
                        name_parts = clean_username.replace('-', ' ').replace('_', ' ').split()
                        if name_parts:
                            name = ' '.join(part.capitalize() for part in name_parts)
                            self._log(f"      📋 Found LinkedIn name from URL: {name}")
                
                if name:
                    people.append({
                        'name': name,
                        'url': href,
                        'source': f'Found on {source_url}'
                    })
                    self._log(f"  ✅ Added LinkedIn person: {name}")
        
        # Also look for plain text LinkedIn URLs (not in <a> tags)
        # This catches URLs that might be in the page as plain text
        text_urls = re.findall(r'https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_-]+', html)
        for url in text_urls:
            if url in found_urls:
                continue
                
            found_urls.add(url)
            username = self._extract_username_from_linkedin_url(url)
            if username:
                clean_username = re.sub(r'[-_][a-zA-Z0-9]+$', '', username)
                name_parts = clean_username.replace('-', ' ').replace('_', ' ').split()
                if name_parts:
                    name = ' '.join(part.capitalize() for part in name_parts)
                    people.append({
                        'name': name,
                        'url': url,
                        'source': f'Found as plain text on {source_url}'
                    })
                    self._log(f"  ✅ Added LinkedIn person from plain text: {name}")
        
        # Remove duplicates by URL (just in case)
        unique_people = {p['url']: p for p in people}.values()
        return list(unique_people)

    def scrape(self, url: str, target_company: str = None) -> Dict:
        # Check cache first
        if url in self.scraped_urls_cache:
            self._log(f"  ⏭️ Skipping {url} (already scraped in this session)")
            return {}
        self.scraped_urls_cache.add(url)

        html = self.fetch_page(url)
        result = {
            'url': url,
            'real_name': None,
            'social_links': [],
            'emails': [],
            'company': None,
            'people_found': []
        }
        # TODO check if this works properly now and does not throw away found accounts cause of the skipping
        # Special handling for LinkedIn - ALWAYS process, even without HTML
        if 'linkedin.com' in url.lower():
            result['social_links'] = [url]
            username = self._extract_username_from_linkedin_url(url)
            if username:
                # Clean up the username - remove trailing numbers/IDs
                clean_username = re.sub(r'[-_][a-zA-Z0-9]+$', '', username)
                name_parts = clean_username.replace('-', ' ').replace('_', ' ').split()
                if name_parts:
                    # Capitalize each part properly
                    result['real_name'] = ' '.join(part.capitalize() for part in name_parts)
                    self._log(f"      📋 LinkedIn name from URL: {result['real_name']}")
            
            # For LinkedIn, we're done - return the result even without HTML
            self._log(f"  ✅ LinkedIn profile processed: {result['real_name'] or username}")
            return result

        # Only process HTML for non-LinkedIn pages
        if html:
            if 'github.com' in url.lower() and '/in/' not in url.lower():
                username = url.rstrip('/').split('/')[-1]
                if username and username not in ['', 'github.com']:
                    github_data = self.extract_github_profile_data(html, username)

                    # GitHub data takes precedence
                    result['real_name'] = github_data.get('real_name')
                    result['company'] = github_data.get('company')
                    result['social_links'] = list(set([url] + github_data.get('social_links', [])))
                    result['emails'] = github_data.get('emails', [])
                    result['people_found'] = self.extract_people_from_page(html, url)

                    self._log(f"  ✅ GitHub extraction complete. Final name: '{result['real_name']}'")
                    return result

            # For non-GitHub pages, use generic extraction
            result['real_name'] = self.extract_real_name(html, url)
            result['social_links'] = self.extract_social_links(html, url)
            result['emails'] = self.extract_emails(html)
            result['company'] = self.extract_company_from_text(html, target_company)
            result['people_found'] = self.extract_people_from_page(html, url)

        return result
    
    def _clean_linkedin_username(self, username: str) -> str:
        if not username:
            return username
        
        # Remove trailing numbers and IDs (e.g., "-ba1b86164", "_12345678")
        clean = re.sub(r'[-_][a-zA-Z0-9]+$', '', username)
        
        # Replace separators with spaces
        clean = clean.replace('-', ' ').replace('_', ' ')
        
        # Capitalize each word
        return ' '.join(part.capitalize() for part in clean.split())