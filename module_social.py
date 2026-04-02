import requests
import pandas as pd
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from ddgs import DDGS
import time
import random
from bs4 import BeautifulSoup


# Each entry: (platform_name, search_suffix, url_pattern_to_validate_result)
# url_pattern must match a direct profile URL, not a post/tag/search page.
_SOCIAL_PLATFORMS = [
    (
        "Facebook",
        "facebook",
        re.compile(r'facebook\.com/(?!pages/|groups/|events/|watch/|marketplace/)([^/?#\s]+)', re.I),
    ),
    (
        "Twitter/X",
        "twitter OR x.com",
        re.compile(r'(?:twitter\.com|x\.com)/(?!search|hashtag|i/|home|explore)([^/?#\s]+)', re.I),
    ),
    (
        "Instagram",
        "instagram",
        re.compile(r'instagram\.com/(?!p/|reel/|explore/|stories/)([^/?#\s]+)', re.I),
    ),
]

# Junk URL fragments that indicate a non-profile page even if domain matches
_JUNK_FRAGMENTS = re.compile(
    r'/posts?/|/videos?/|/photos?/|/reels?/|/stories/|'
    r'[?&](trk|utm|ref|src|fref)=|/events?/|/groups?/|'
    r'activity-\d+|/hashtag/|/search/|/explore/',
    re.I,
)


def _is_valid_profile_url(url: str, platform_pattern: re.Pattern) -> bool:
    if not url or not isinstance(url, str):
        return False
    if _JUNK_FRAGMENTS.search(url):
        return False
    return bool(platform_pattern.search(url))


class SocialScanner:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/617.1",
            "Mozilla/5.0 (X11; Linux x86_64) Firefox/121.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15",
        ]
        self.session = requests.Session()
        self.last_request = 0
        self.min_delay = 5

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
            'Ãƒ': 'Ã', 'Â': '', 'â€"': '-',
            'â€™': "'", 'â€˜': "'", 'â€œ': '"', 'â€': '"',
        }
        for wrong, correct in replacements.items():
            text = text.replace(wrong, correct)
        try:
            text = text.encode('latin1').decode('utf-8', errors='ignore')
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
        return text.strip()

    def _rate_limit(self):
        now = time.time()
        if now - self.last_request < self.min_delay:
            time.sleep(self.min_delay - (now - self.last_request))
        self.last_request = time.time()

    def _get_headers(self):
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
        }

    # General web search
    def search_entity_globally(self, entity_name: str, target_company: str) -> List[Dict]:
        findings = []
        search_query = f'{entity_name} {target_company}'
        print(f"  🔍 Web search for: {search_query}")

        try:
            ddg_results = self._search_duckduckgo_global(
                search_query, entity_name, target_company
            )
            findings.extend(ddg_results)
            print(f"    ✓ DuckDuckGo: {len(ddg_results)} results")
        except Exception as e:
            print(f"    ⚠️ DuckDuckGo error: {e}")

        if len(findings) < 5:
            try:
                bing_results = self._search_bing_global(
                    search_query, entity_name, target_company
                )
                findings.extend(bing_results)
                print(f"    ✓ Bing: {len(bing_results)} results")
            except Exception as e:
                print(f"    ⚠️ Bing error: {e}")

        if len(findings) < 3:
            try:
                yahoo_results = self._search_yahoo_global(
                    search_query, entity_name, target_company
                )
                findings.extend(yahoo_results)
                print(f"    ✓ Yahoo: {len(yahoo_results)} results")
            except Exception as e:
                print(f"    ⚠️ Yahoo error: {e}")

        return findings

    # targeted social media profile search 
    def search_social_profiles(
        self, entity_name: str, target_company: str
    ) -> List[str]:
        found_urls: List[str] = []

        for platform_name, search_suffix, url_pattern in _SOCIAL_PLATFORMS:
            query = f'"{entity_name}" "{target_company}" {search_suffix}'
            print(f"  🔍 Social search [{platform_name}]: {query}")

            try:
                with DDGS() as ddgs:
                    results = list(ddgs.text(query, max_results=5, timeout=10))

                for r in results:
                    if not r or not isinstance(r, dict):
                        continue
                    link = r.get('href', '')
                    if _is_valid_profile_url(link, url_pattern):
                        if link not in found_urls:
                            print(f"    ✅ [{platform_name}] found profile: {link}")
                            found_urls.append(link)
                        # One confirmed profile per platform per person is enough
                        break

            except Exception as e:
                print(f"    ⚠️ [{platform_name}] search error: {e}")

            # Brief pause between platform searches to avoid rate-limiting
            time.sleep(1.5)

        return found_urls

    def _search_duckduckgo_global(
        self, query: str, entity_name: str, target_company: str
    ) -> List[Dict]:
        findings = []
        try:
            with DDGS() as ddgs:
                ddgs_gen = ddgs.text(query, max_results=5, timeout=10)
                if ddgs_gen:
                    for r in ddgs_gen:
                        if r and isinstance(r, dict):
                            findings.append({
                                "Title":          self._fix_encoding(r.get('title', '')),
                                "Link":           r.get('href', ''),
                                "Snippet":        self._fix_encoding(r.get('body', '')),
                                "Platform":       "Web",
                                "Entity_Name":    entity_name,
                                "Target_Company": target_company,
                                "Source":         "DuckDuckGo",
                                "Timestamp":      datetime.now().isoformat(),
                            })
        except Exception as e:
            print(f"    ⚠️ DDG search error: {e}")
        return findings

    def _search_bing_global(
        self, query: str, entity_name: str, target_company: str
    ) -> List[Dict]:
        findings = []
        try:
            self._rate_limit()
            response = self.session.get(
                'https://www.bing.com/search',
                params={'q': query, 'count': 5},
                headers=self._get_headers(),
                timeout=10,
            )
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                for result in soup.select('li.b_algo')[:5]:
                    link_elem = result.select_one('h2 a')
                    if not link_elem:
                        continue
                    snippet_elem = result.select_one('p')
                    findings.append({
                        "Title":          link_elem.text,
                        "Link":           link_elem.get('href', ''),
                        "Snippet":        snippet_elem.text if snippet_elem else '',
                        "Platform":       "Web",
                        "Entity_Name":    entity_name,
                        "Target_Company": target_company,
                        "Source":         "Bing",
                        "Timestamp":      datetime.now().isoformat(),
                    })
        except Exception as e:
            print(f"    ⚠️ Bing search error: {e}")
        return findings

    def _search_yahoo_global(
        self, query: str, entity_name: str, target_company: str
    ) -> List[Dict]:
        findings = []
        try:
            self._rate_limit()
            response = self.session.get(
                'https://search.yahoo.com/search',
                params={'p': query, 'output': 'rss', 'n': 5},
                headers=self._get_headers(),
                timeout=10,
            )
            if response.status_code == 200:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(response.text)
                for item in root.findall('.//item')[:5]:
                    findings.append({
                        "Title": (
                            item.find('title').text
                            if item.find('title') is not None else ''
                        ),
                        "Link": (
                            item.find('link').text
                            if item.find('link') is not None else ''
                        ),
                        "Snippet": (
                            item.find('description').text
                            if item.find('description') is not None else ''
                        ),
                        "Platform":       "Web",
                        "Entity_Name":    entity_name,
                        "Target_Company": target_company,
                        "Source":         "Yahoo",
                        "Timestamp":      datetime.now().isoformat(),
                    })
        except Exception as e:
            print(f"    ⚠️ Yahoo search error: {e}")
        return findings

    def batch_search_entities(
        self, entities: List[Dict], target_company: str
    ) -> pd.DataFrame:
        all_findings = []
        for entity in entities:
            name     = entity.get('name', '')
            username = entity.get('username', '')
            if name:
                all_findings.extend(self.search_entity_globally(name, target_company))
            if username and username.lower() != name.lower():
                all_findings.extend(
                    self.search_entity_globally(username, target_company)
                )
        return pd.DataFrame(all_findings) if all_findings else pd.DataFrame()

    def verify_profile_employment(
        self, profile_url: str, target_company: str
    ) -> Dict:
        try:
            self._rate_limit()
            response = self.session.get(
                profile_url, headers=self._get_headers(), timeout=10
            )
            if response.status_code == 200:
                if target_company.lower() in response.text.lower():
                    return {
                        'profile_url':    profile_url,
                        'target_company': target_company,
                        'verified':       True,
                        'confidence':     'Medium',
                        'evidence':       'Company name found on profile page',
                        'method':         'Page Scan',
                    }
        except Exception:
            pass
        return {
            'profile_url':    profile_url,
            'target_company': target_company,
            'verified':       False,
            'confidence':     'Low',
            'evidence':       '',
            'method':         'Page Scan',
        }