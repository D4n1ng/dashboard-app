import requests
import pandas as pd
import re
from typing import List, Dict, Any
from datetime import datetime
from ddgs import DDGS
import time
import random
from bs4 import BeautifulSoup

class SocialScanner:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/617.1",
            "Mozilla/5.0 (X11; Linux x86_64) Firefox/121.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15"
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
            'Ãƒ': 'Ã', 'Â': '', 'â€“': '-', 'â€”': '-',
            'â€™': "'", 'â€˜': "'", 'â€œ': '"', 'â€': '"'
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
            'Connection': 'keep-alive'
        }

    def search_entity_globally(self, entity_name, target_company):
        findings = []
        
        # Build the search query - name + company
        search_query = f'{entity_name} {target_company}'
        print(f"  🔍 Web search for: {search_query}")
        
        # Try DuckDuckGo first
        try:
            ddg_results = self._search_duckduckgo_global(search_query, entity_name, target_company)
            findings.extend(ddg_results)
            print(f"    ✓ DuckDuckGo: {len(ddg_results)} results")
        except Exception as e:
            print(f"    ⚠️ DuckDuckGo error: {e}")
        
        # Try Bing as fallback
        if len(findings) < 5:
            try:
                bing_results = self._search_bing_global(search_query, entity_name, target_company)
                findings.extend(bing_results)
                print(f"    ✓ Bing: {len(bing_results)} results")
            except Exception as e:
                print(f"    ⚠️ Bing error: {e}")
        
        # Try Yahoo as last resort
        if len(findings) < 3:
            try:
                yahoo_results = self._search_yahoo_global(search_query, entity_name, target_company)
                findings.extend(yahoo_results)
                print(f"    ✓ Yahoo: {len(yahoo_results)} results")
            except Exception as e:
                print(f"    ⚠️ Yahoo error: {e}")
        
        return findings

    def _search_duckduckgo_global(self, query, entity_name, target_company):
        findings = []
        try:
            with DDGS() as ddgs:
                # No platform filtering - just general web search
                ddgs_gen = ddgs.text(query, max_results=5, timeout=10)
                if ddgs_gen:
                    for r in ddgs_gen:
                        if r and isinstance(r, dict):
                            title = self._fix_encoding(r.get('title', ''))
                            snippet = self._fix_encoding(r.get('body', ''))
                            link = r.get('href', '')

                            findings.append({
                                "Title": title,
                                "Link": link,
                                "Snippet": snippet,
                                "Platform": "Web",
                                "Entity_Name": entity_name,
                                "Target_Company": target_company,
                                "Source": "DuckDuckGo",
                                "Timestamp": datetime.now().isoformat()
                            })
        except Exception as e:
            print(f"    ⚠️ DDG search error: {e}")
        return findings

    def _search_bing_global(self, query, entity_name, target_company):
        findings = []
        try:
            self._rate_limit()
            params = {
                'q': query,
                'count': 5
            }
            
            response = self.session.get(
                'https://www.bing.com/search',
                params=params,
                headers=self._get_headers(),
                timeout=10
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                for result in soup.select('li.b_algo')[:5]:
                    link_elem = result.select_one('h2 a')
                    if not link_elem:
                        continue
                    
                    url = link_elem.get('href', '')
                    title = link_elem.text
                    snippet_elem = result.select_one('p')
                    snippet = snippet_elem.text if snippet_elem else ''
                    
                    findings.append({
                        "Title": title,
                        "Link": url,
                        "Snippet": snippet,
                        "Platform": "Web",
                        "Entity_Name": entity_name,
                        "Target_Company": target_company,
                        "Source": "Bing",
                        "Timestamp": datetime.now().isoformat()
                    })
        except Exception as e:
            print(f"    ⚠️ Bing search error: {e}")
        return findings

    def _search_yahoo_global(self, query, entity_name, target_company):
        findings = []
        try:
            self._rate_limit()
            params = {
                'p': query,
                'output': 'rss',
                'n': 5
            }
            
            response = self.session.get(
                'https://search.yahoo.com/search',
                params=params,
                headers=self._get_headers(),
                timeout=10
            )
            
            if response.status_code == 200:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(response.text)
                
                for item in root.findall('.//item')[:5]:
                    title = item.find('title').text if item.find('title') is not None else ''
                    link = item.find('link').text if item.find('link') is not None else ''
                    description = item.find('description').text if item.find('description') is not None else ''
                    
                    findings.append({
                        "Title": title,
                        "Link": link,
                        "Snippet": description,
                        "Platform": "Web",
                        "Entity_Name": entity_name,
                        "Target_Company": target_company,
                        "Source": "Yahoo",
                        "Timestamp": datetime.now().isoformat()
                    })
        except Exception as e:
            print(f"    ⚠️ Yahoo search error: {e}")
        return findings

    def batch_search_entities(self, entities: List[Dict], target_company: str) -> pd.DataFrame:
        all_findings = []
        
        for entity in entities:
            name = entity.get('name', '')
            username = entity.get('username', '')
            
            if name:
                findings = self.search_entity_globally(name, target_company)
                all_findings.extend(findings)
            
            if username and username.lower() != name.lower():
                findings = self.search_entity_globally(username, target_company)
                all_findings.extend(findings)
        
        if all_findings:
            return pd.DataFrame(all_findings)
        return pd.DataFrame()

    def verify_profile_employment(self, profile_url: str, target_company: str) -> Dict:
        try:
            self._rate_limit()
            response = self.session.get(
                profile_url,
                headers=self._get_headers(),
                timeout=10
            )
            
            if response.status_code == 200:
                if target_company.lower() in response.text.lower():
                    return {
                        'profile_url': profile_url,
                        'target_company': target_company,
                        'verified': True,
                        'confidence': 'Medium',
                        'evidence': 'Company name found on profile page',
                        'method': 'Page Scan'
                    }
        except Exception as e:
            pass
        
        return {
            'profile_url': profile_url,
            'target_company': target_company,
            'verified': False,
            'confidence': 'Low',
            'evidence': '',
            'method': 'Page Scan'
        }