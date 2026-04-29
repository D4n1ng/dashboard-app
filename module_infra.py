import dns.resolver
import requests
from ddgs import DDGS
import json
import socket
import whois  


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
                # Google's internal APIs start with ")]}'" to prevent JSON hijacking. We strip it.
                raw_data = response.text.replace(")]}'", "").strip()
                
                try:
                    data = json.loads(raw_data)
                    inner = data[0]
                    # Was not able to 100% verify the meaning but current logic should be correct for most based on some testing
                    # Format: ["sb.ssr", status_code, is_malware, is_phishing, has_unsafe, ...]
                    status_code = inner[1]
                    has_unsafe  = inner[4] if len(inner) > 4 else False
                    
                    print(f"🧪 status_code: {status_code}, has_unsafe: {has_unsafe}")

                    if status_code == 1:
                        return {"status": "✅ Safe (No unsafe content found)", "url": frontend_url}

                    elif status_code == 2:
                        return {"status": "❌ DANGEROUS (Unsafe content found!)", "url": frontend_url}

                    elif status_code == 3:
                        return {"status": "⚠️ WARNING (Some pages on this site are unsafe)", "url": frontend_url}

                    elif status_code == 5:
                        return {"status": "❓ Unknown (Check manually)", "url": frontend_url}
                    
                    elif response.status_code == 429:
                        return {"status": "⚠️ Rate limited (try again later)", "url": frontend_url}
                    
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
    
    def analyze_web_headers(self, use_head_request=False):
        tech_found = []
        try:
            # User-Agent to avoid immediate blocking
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            if use_head_request:
                response = requests.head(self.url, timeout=5, headers=headers, allow_redirects=True)
            else:
                response = requests.get(self.url, timeout=5, headers=headers)
            
            response_headers = response.headers
            
            # Server-Software 
            if "Server" in response_headers:
                tech_found.append({"Software": f"Server: {response_headers['Server']}", "Risk": "Info"})
            
            # Frameworks 
            if "X-Powered-By" in response_headers:
                tech_found.append({"Software": response_headers['X-Powered-By'], "Risk": "Medium"})
            
            # Security Features
            if "Strict-Transport-Security" in response_headers:
                tech_found.append({"Software": "HSTS Security", "Risk": "Low"})
            
            # CDN Detection
            if "CF-Ray" in response_headers or "cf-ray" in response_headers:
                tech_found.append({"Software": "Cloudflare CDN", "Risk": "Info"})
            if "X-Amz-Cf-Id" in response_headers:
                tech_found.append({"Software": "AWS CloudFront CDN", "Risk": "Info"})
            if "X-Azure-Ref" in response_headers:
                tech_found.append({"Software": "Azure CDN", "Risk": "Info"})

            # Body analysis (only if we used GET request)
            if not use_head_request:
                body = response.text.lower()
                if "wp-content" in body:
                    tech_found.append({"Software": "WordPress CMS", "Risk": "Medium"})
                if "react" in body or "react-dom" in body:
                    tech_found.append({"Software": "React Frontend", "Risk": "Low"})
                if "angular" in body:
                    tech_found.append({"Software": "Angular Frontend", "Risk": "Low"})
                if "vue.js" in body or "_vue" in body:
                    tech_found.append({"Software": "Vue.js Frontend", "Risk": "Low"})

        except Exception as e:
            print(f"Web-Header Scan failed: {e}")
        
        return tech_found

    def analyze_dns_txt(self):
        # DNS analysis including TXT, MX, DMARC, and DKIM records.
        print(f"Analyzing DNS Records for {self.domain}...")
        found_software = []
        
        # Check TXT records
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
            print(f"TXT DNS Error: {e}")
        
        # Check MX records (Mail Exchange)
        try:
            mx_answers = dns.resolver.resolve(self.domain, 'MX')
            mx_servers = [str(rdata.exchange).rstrip('.') for rdata in mx_answers]
            if mx_servers:
                # Detect mail provider from MX records
                mx_string = ' '.join(mx_servers).lower()
                if 'google' in mx_string:
                    found_software.append({"Software": "Gmail/Google Workspace Mail", "Risk": "Low"})
                elif 'outlook' in mx_string or 'microsoft' in mx_string:
                    found_software.append({"Software": "Microsoft 365 Mail", "Risk": "Low"})
                elif 'proofpoint' in mx_string:
                    found_software.append({"Software": "Proofpoint Email Security", "Risk": "Low"})
                else:
                    found_software.append({"Software": f"Mail Exchange: {mx_servers[0]}", "Risk": "Low"})
        except Exception as e:
            print(f"MX DNS Error: {e}")
        
        # Check DMARC record
        try:
            dmarc_domain = f"_dmarc.{self.domain}"
            dmarc_answers = dns.resolver.resolve(dmarc_domain, 'TXT')
            for rdata in dmarc_answers:
                dmarc_record = rdata.to_text().strip('"')
                if "v=DMARC1" in dmarc_record:
                    # Extract policy if available
                    if "p=reject" in dmarc_record:
                        found_software.append({"Software": "DMARC Policy: Reject", "Risk": "Low"})
                    elif "p=quarantine" in dmarc_record:
                        found_software.append({"Software": "DMARC Policy: Quarantine", "Risk": "Low"})
                    elif "p=none" in dmarc_record:
                        found_software.append({"Software": "DMARC Policy: Monitor Only", "Risk": "Medium"})
                    else:
                        found_software.append({"Software": "DMARC Policy Enabled", "Risk": "Low"})
                    break
        except Exception as e:
            print(f"DMARC DNS Error: {e}")
        
        # Check DKIM record (common selectors)
        try:
            common_selectors = ['default', 'google', 'k1', 'selector1', 'selector2', 's1', 's2']
            dkim_found = False
            for selector in common_selectors:
                dkim_domain = f"{selector}._domainkey.{self.domain}"
                try:
                    dkim_answers = dns.resolver.resolve(dkim_domain, 'TXT')
                    for rdata in dkim_answers:
                        dkim_record = rdata.to_text().strip('"')
                        if "v=DKIM1" in dkim_record or "k=rsa" in dkim_record or "p=" in dkim_record:
                            found_software.append({"Software": f"DKIM Key ({selector})", "Risk": "Low"})
                            dkim_found = True
                            break
                    if dkim_found:
                        break
                except:
                    continue
        except Exception as e:
            print(f"DKIM DNS Error: {e}")
            
        return found_software

    def check_subdomains(self):
        common_subs = ["vpn", "jira", "hr", "personio", "mail", "dev", "git", "test", 
                       "staging", "admin", "portal", "internal", "confluence", "jenkins"]
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
        # Run all infrastructure analysis modules and return comprehensive results.
        return {
            "dns": self.analyze_dns_txt(),
            "web": self.analyze_web_headers(),
            "subdomains": self.check_subdomains(),
            "safe_search": self.check_safe_browsing(self.domain)
        }

class CompanyEnricher:
    
    def get_whois_info(self, domain):
        # Get registrar information via WHOIS lookup.
        try:
            w = whois.whois(domain)
            return {
                "registrar": w.registrar if hasattr(w, 'registrar') else "Unknown",
                "creation_date": str(w.creation_date[0]) if isinstance(w.creation_date, list) else str(w.creation_date) if w.creation_date else "Unknown",
                "expiration_date": str(w.expiration_date[0]) if isinstance(w.expiration_date, list) else str(w.expiration_date) if w.expiration_date else "Unknown"
            }
        except Exception as e:
            print(f"WHOIS lookup failed: {e}")
            return {
                "registrar": "Unable to determine",
                "creation_date": "Unknown",
                "expiration_date": "Unknown"
            }
    
    def get_asn_info(self, domain):
        # Get ASN (Autonomous System Number) information by resolving domain IP
        # and checking against public ASN databases.

        try:
            # Resolve domain to IP
            ip = socket.gethostbyname(domain)
            
            # Use ipinfo.io free API for ASN lookup (no key required for basic info)
            response = requests.get(f"https://ipinfo.io/{ip}/json", timeout=5)
            if response.status_code == 200:
                data = response.json()
                return {
                    "ip": ip,
                    "asn": data.get("org", "Unknown"),
                    "location": f"{data.get('city', 'Unknown')}, {data.get('country', 'Unknown')}"
                }
        except Exception as e:
            print(f"ASN lookup failed: {e}")
        
        return {
            "ip": "Unable to resolve",
            "asn": "Unknown",
            "location": "Unknown"
        }
    
    def get_details(self, domain):
        company_name = domain.split('.')[0].title()
        
        # Get WHOIS/registrar info
        whois_info = self.get_whois_info(domain)
        
        # Get ASN info
        asn_info = self.get_asn_info(domain)

        return {
            "name": company_name,
            "employees": "Estimation via OSINT",
            "registrar": whois_info["registrar"],
            "domain_created": whois_info["creation_date"],
            "domain_expires": whois_info["expiration_date"],
            "ip_address": asn_info["ip"],
            "asn": asn_info["asn"],
            "location": asn_info["location"]
        }