import dns.resolver
import requests
from ddgs import DDGS

class InfraScanner:
    def __init__(self, domain):
        self.domain = domain
        self.url = f"https://{domain}"


    def check_safe_browsing(self, domain):
        # The URL the user sees
        frontend_url = f"https://transparencyreport.google.com/safe-browsing/search?url={domain}&hl=en"
        
        # The actual background API URL the page uses to get the data
        api_url = f"https://transparencyreport.google.com/transparencyreport/api/v3/safebrowsing/status?site={domain}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        try:
            print(f"🔍 Checking Google Safe Search for: {domain}")
            response = requests.get(api_url, headers=headers, timeout=10)
            print(f"📡 Response status: {response.status_code}")

            if response.status_code == 200:
                import json
                # Google's internal APIs start with ")]}'" to prevent JSON hijacking. We strip it.
                raw_data = response.text.replace(")]}'", "").strip()
                
                try:
                    data = json.loads(raw_data)
                    inner = data[0]
                    # TODO only speculation check via different urls for potentially more detailed info on what is unsafe
                    # TODO Sometimes 429 (might not be fixable)
                    # Format: ["sb.ssr", status_code, is_malware, is_phishing, has_unsafe, ...]
                    # has_unsafe (index 4): true if any unsafe content detected
                    status_code = inner[1]
                    has_unsafe  = inner[4] if len(inner) > 4 else False
                    
                    print(f"🧪 status_code: {status_code}, has_unsafe: {has_unsafe}")

                    if status_code == 1 and not has_unsafe:
                        return {"status": "✅ Safe (No unsafe content found)", "url": frontend_url}
                    elif status_code == 2 or (status_code != 1 and has_unsafe):
                        return {"status": "❌ DANGEROUS (Unsafe content found!)", "url": frontend_url}
                    elif status_code == 3:
                        return {"status": "⚠️ WARNING (Some pages on this site are unsafe)", "url": frontend_url}
                    elif status_code == 5:
                        return {"status": "❓ Unknown (Check manually)", "url": frontend_url}
                    else:
                        return {"status": f"❓ Unexpected status code: {status_code}", "url": frontend_url}
                        
                except (json.JSONDecodeError, IndexError, TypeError) as e:
                    print(f"❌ Parse error: {e} | raw: {raw_data[:200]}")
                    return {"status": "❓ Could not parse Google response", "url": frontend_url}
            else:
                return {"status": f"❌ Error: HTTP {response.status_code}", "url": frontend_url}
                
        except Exception as e:
            print(f"❌ Connection Error: {e}")
            return {"status": f"❌ Connection Error: {str(e)}", "url": frontend_url}
    
    def analyze_web_headers(self):
        tech_found = []
        try:
            # Gängiger User-Agent, um nicht sofort blockiert zu werden
            response = requests.get(self.url, timeout=5, headers={'User-Agent': 'Mozilla/5.0'})
            headers = response.headers
            
            # Server-Software 
            if "Server" in headers:
                tech_found.append({"Software": f"Server: {headers['Server']}", "Risk": "Info"})
            
            # Frameworks 
            if "X-Powered-By" in headers:
                tech_found.append({"Software": headers['X-Powered-By'], "Risk": "Medium"})
            
            # Sicherheits-Features
            if "Strict-Transport-Security" in headers:
                tech_found.append({"Software": "HSTS Security", "Risk": "Low"})

            # Body-Analyse 
            body = response.text.lower()
            if "wp-content" in body:
                tech_found.append({"Software": "WordPress CMS", "Risk": "Medium"})
            if "react" in body or "react-dom" in body:
                tech_found.append({"Software": "React Frontend", "Risk": "Low"})

        except Exception as e:
            print(f"Web-Header Scan failed: {e}")
        
        return tech_found

    def analyze_dns_txt(self):
        print(f"Analyzing DNS Records for {self.domain}...")
        found_software = []
        
        try:
            answers = dns.resolver.resolve(self.domain, 'TXT')
            for rdata in answers:
                txt_record = rdata.to_text().strip('"')
                
                if "google-site-verification" in txt_record:
                    found_software.append({"Software": "Google Workspace", "Risk": "Low"})
                if "outlook" in txt_record or "protection.outlook.com" in txt_record:
                    found_software.append({"Software": "Microsoft Office 365", "Risk": "Low"})
                if "atlassian" in txt_record:
                    found_software.append({"Software": "Atlassian Cloud", "Risk": "Medium"})
                if "v=spf1" in txt_record:
                    found_software.append({"Software": "SPF Mail Security", "Risk": "Low"})

        except Exception as e:
            print(f"DNS Error: {e}")
            
        return found_software

    def check_subdomains(self):
        common_subs = ["vpn", "jira", "wiki", "hr", "personio", "mail", "dev", "git", "test"]
        found_portals = []
        
        for sub in common_subs:
            hostname = f"{sub}.{self.domain}"
            try:
                dns.resolver.resolve(hostname, 'A')
                found_portals.append({"Portal": hostname, "Risk": "High (Login Portal exposed)"})
            except:
                pass 
        
        return found_portals
    
    def analyze_all(self):
        return {
            "dns": self.analyze_dns_txt(),
            "web": self.analyze_web_headers(),
            "subdomains": self.check_subdomains(),
            "safe_search": self.check_safe_browsing(self.domain)
        }

class CompanyEnricher:
    def get_details(self, domain):
        company_name = domain.split('.')[0].title()
        description = "No description found."
        
        try:
            with DDGS() as ddgs:
                # Suche nach dem Firmenprofil für eine Kurzbeschreibung
                query = f"{company_name} company profile information"
                results = list(ddgs.text(query, max_results=1))
                if results:
                    description = results[0]['body'][:250] + "..."
        except:
            pass

        return {
            "name": company_name,
            "description": description,
            "employees": "Estimation via OSINT",
            "linkedin": f"https://www.linkedin.com/company/{company_name.lower()}"
        }
