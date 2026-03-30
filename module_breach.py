import requests
import time
import urllib.parse

class BreachChecker:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.base_url = "https://haveibeenpwned.com/api/v3"
        self.unified_search_url = "https://haveibeenpwned.com:443/unifiedsearch/"
        self.headers = {
            'hibp-api-key': api_key,
            'user-agent': 'IDP-Student-Project'
        }
        # Browser-like headers for the no-API method
        self.browser_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/100.0",
            "Accept": "*/*", 
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://haveibeenpwned.com/",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Te": "trailers"
        }
        self.sleep_interval = 2  # Be nice to the service
    
    def check_hibp_api(self, email):
        url = f"https://haveibeenpwned.com/api/v3/breachedaccount/{email}"
        headers = {"hibp-api-key": self.api_key, "user-agent": "OSINT-Project"}
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()  # List of breaches
        elif response.status_code == 404:
            return []  # Clean
        return None  # Error
    
    def check_hibp_no_api(self, account):
        try:
            # URL encode the account (email or phone)
            search_url = self.unified_search_url + urllib.parse.quote(account.strip())
            
            # Add a small delay to be respectful
            time.sleep(self.sleep_interval)
            
            response = requests.get(search_url, headers=self.browser_headers)
            
            # 200 = found in breaches, 404 = not found
            if response.status_code == 200:
                return True, response.json() if response.text else []
            elif response.status_code == 404:
                return False, []
            else:
                return None, {"status": "error", "code": response.status_code}
        except Exception as e:
            return None, {"status": "error", "message": str(e)}
    
    def check_email(self, email, use_api_if_available=True):
        # Try API method first if we have a key and want to use it
        if use_api_if_available and self.api_key:
            print(f"🔑 Using API method for {email}...")
            return self._check_email_api(email)
        else:
            print(f"🌐 Using web method for {email}...")
            return self._check_email_web(email)
    
    def _check_email_api(self, email):
        print(f"Check leak status for {email} via API...")
        url = f"{self.base_url}/breachedaccount/{email}?truncateResponse=false"
        
        try:
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                leaks = response.json()
                return {
                    "status": "leaked", 
                    "count": len(leaks), 
                    "details": [l['Name'] for l in leaks],
                    "method": "api"
                }
            elif response.status_code == 404:
                return {"status": "safe", "count": 0, "method": "api"}
            elif response.status_code == 429:
                print("⚠️ Rate Limit reached. Wait 2 seconds...")
                time.sleep(2)
                return self._check_email_api(email)
            else:
                return {"status": "error", "code": response.status_code, "method": "api"}
        except Exception as e:
            return {"status": "error", "message": str(e), "method": "api"}
    
    def _check_email_web(self, email):
        print(f"🔍 Check leak status for {email} via web method...")
        
        try:
            found, data = self.check_hibp_no_api(email)
            
            if found is True:
                # Parse the breach data if available
                if data and isinstance(data, list):
                    breach_names = [b.get('Name', 'Unknown') for b in data if isinstance(b, dict)]
                    return {
                        "status": "leaked",
                        "count": len(breach_names),
                        "details": breach_names,
                        "method": "web"
                    }
                else:
                    return {
                        "status": "leaked",
                        "count": 1,
                        "details": ["Unknown Breach"],
                        "method": "web"
                    }
            elif found is False:
                return {"status": "safe", "count": 0, "method": "web"}
            else:
                # Error case
                return {"status": "error", "details": data, "method": "web"}
                
        except Exception as e:
            return {"status": "error", "message": str(e), "method": "web"}
    
    def batch_check(self, accounts, use_api_if_available=True, max_workers=1):
        results = {}
        for account in accounts:
            results[account] = self.check_email(account, use_api_if_available)
            # Add a small delay between checks to avoid rate limiting
            time.sleep(1)
        return results


# Test the module
if __name__ == "__main__":
    # Test with and without API key
    print("=" * 50)
    print("Testing BreachChecker")
    print("=" * 50)
    
    # Test 1: Without API key (web method)
    print("\n📱 Test 1: No API Key (Web Method)")
    checker_no_api = BreachChecker()  # No API key
    test_email = "test@example.com"  # This email is likely in breaches
    result = checker_no_api.check_email(test_email, use_api_if_available=False)
    print(f"Result for {test_email}: {result}")
    
    # Test 2: With a real email that might be breached
    print("\n📱 Test 2: Check a real email")
    # You can replace this with an email you want to test
    real_test = "jonas@michel.coffee"  # From Simorenarium's profile!
    result = checker_no_api.check_email(real_test, use_api_if_available=False)
    print(f"Result for {real_test}: {result}")
    
    # Test 3: Batch check
    print("\n📱 Test 3: Batch Check")
    test_accounts = [
        "test@example.com",
        "hello@world.com",
        "admin@example.com"
    ]
    batch_results = checker_no_api.batch_check(test_accounts, use_api_if_available=False)
    for account, status in batch_results.items():
        print(f"  {account}: {status}")