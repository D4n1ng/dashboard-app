import re
from typing import List, Dict, Set
from dataclasses import dataclass
from datetime import datetime

@dataclass
class SecretFinding:
    type: str
    value: str
    file_path: str
    line_number: int
    context: str
    risk_level: str 
    timestamp: str

class SecretDetector:
    # Detect hardcoded secrets, credentials, and sensitive data in code
    
    # Secret patterns (regex)
    SECRET_PATTERNS = {
        # AWS Keys
        'AWS_ACCESS_KEY': {
            'pattern': re.compile(r'AKIA[0-9A-Z]{16}'),
            'risk': 'CRITICAL',
            'description': 'AWS Access Key ID'
        },
        'AWS_SECRET_KEY': {
            'pattern': re.compile(r'[A-Za-z0-9/+=]{40}'),
            'risk': 'CRITICAL',
            'description': 'AWS Secret Access Key'
        },
        
        # API Keys
        'GITHUB_TOKEN': {
            'pattern': re.compile(r'ghp_[A-Za-z0-9]{36}'),
            'risk': 'CRITICAL',
            'description': 'GitHub Personal Access Token'
        },
        'SLACK_TOKEN': {
            'pattern': re.compile(r'xox[baprs]-[0-9a-zA-Z]{10,48}'),
            'risk': 'HIGH',
            'description': 'Slack Bot/App Token'
        },
        'STRIPE_API_KEY': {
            'pattern': re.compile(r'sk_live_[0-9a-zA-Z]{24}'),
            'risk': 'CRITICAL',
            'description': 'Stripe Live Secret Key'
        },
        'STRIPE_TEST_KEY': {
            'pattern': re.compile(r'sk_test_[0-9a-zA-Z]{24}'),
            'risk': 'MEDIUM',
            'description': 'Stripe Test Secret Key'
        },
        'GOOGLE_API_KEY': {
            'pattern': re.compile(r'AIza[0-9A-Za-z\-_]{35}'),
            'risk': 'HIGH',
            'description': 'Google API Key'
        },
        'FACEBOOK_APP_SECRET': {
            'pattern': re.compile(r'[0-9a-f]{32}'),
            'risk': 'HIGH',
            'description': 'Facebook App Secret'
        },
        
        # Tokens
        'JWT_TOKEN': {
            'pattern': re.compile(r'eyJ[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*'),
            'risk': 'HIGH',
            'description': 'JWT Token'
        },
        'GENERIC_TOKEN': {
            'pattern': re.compile(r'token[\s:=]+[A-Za-z0-9+/=]{20,}'),
            'risk': 'MEDIUM',
            'description': 'Generic Token/String'
        },
        
        # Passwords
        'PASSWORD_IN_STRING': {
            'pattern': re.compile(r'(?:password|passwd|pwd)\s*[:=]\s*["\']([^"\']{8,})["\']', re.IGNORECASE),
            'risk': 'CRITICAL',
            'description': 'Password in string literal'
        },
        'DATABASE_URL': {
            'pattern': re.compile(r'(?:postgresql|mysql|mongodb)://[^/\s]+:[^@\s]+@'),
            'risk': 'CRITICAL',
            'description': 'Database connection string with credentials'
        },
        
        # Private Keys
        'PRIVATE_KEY_BLOCK': {
            'pattern': re.compile(r'-----BEGIN (?:RSA|DSA|EC|OPENSSH) PRIVATE KEY-----'),
            'risk': 'CRITICAL',
            'description': 'Private Key Block'
        },
        
        # Webhooks
        'SLACK_WEBHOOK': {
            'pattern': re.compile(r'https://hooks\.slack\.com/services/[A-Z0-9]+/[A-Z0-9]+/[A-Za-z0-9]+'),
            'risk': 'HIGH',
            'description': 'Slack Webhook URL'
        },
        'DISCORD_WEBHOOK': {
            'pattern': re.compile(r'https://discord\.com/api/webhooks/[0-9]+/[A-Za-z0-9_-]+'),
            'risk': 'HIGH',
            'description': 'Discord Webhook URL'
        },
        
        # Environment files
        'ENV_VAR_EXPORT': {
            'pattern': re.compile(r'export\s+[A-Z_]+=.*?["\'].*?(?<!\\)["\']'),
            'risk': 'MEDIUM',
            'description': 'Environment variable export'
        },
        
        # IP Addresses (internal)
        'INTERNAL_IP': {
            'pattern': re.compile(r'(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2[0-9]|3[0-1])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})'),
            'risk': 'LOW',
            'description': 'Internal IP Address'
        },
    }
    
    # File extensions to scan
    SCAN_EXTENSIONS = {
        '.py', '.js', '.ts', '.java', '.go', '.rb', '.php',
        '.env', '.json', '.yml', '.yaml', '.xml', '.conf',
        '.config', '.ini', '.txt', '.md', '.sh', '.bat'
    }
    
    # Files to prioritize (high risk)
    HIGH_PRIORITY_FILES = {
        '.env', 'config.py', 'settings.py', 'secrets.py',
        'credentials.json', '.env.local', '.env.production'
    }
    
    def __init__(self):
        self.findings: List[SecretFinding] = []
        self.compiled_patterns = self.SECRET_PATTERNS
    
    def scan_content(self, content: str, file_path: str = "unknown") -> List[SecretFinding]:
        findings = []
        lines = content.split('\n')
        
        for line_num, line in enumerate(lines, 1):
            for secret_type, info in self.compiled_patterns.items():
                matches = info['pattern'].findall(line)
                if matches:
                    for match in matches if isinstance(matches, list) else [matches]:
                        # Convert match to string if it's a tuple
                        match_str = str(match) if not isinstance(match, str) else match
                        
                        finding = SecretFinding(
                            type=secret_type,
                            value=self._mask_secret(match_str),
                            file_path=file_path,
                            line_number=line_num,
                            context=self._get_context(line, match_str),
                            risk_level=info['risk'],
                            timestamp=datetime.now().isoformat()
                        )
                        findings.append(finding)
        
        return findings
    
    def scan_repository_files(self, repo_files: Dict[str, str]) -> List[SecretFinding]:
        all_findings = []
        
        # Prioritize high-risk files first
        prioritized_files = []
        other_files = []
        
        for file_path in repo_files.keys():
            is_high_priority = any(
                file_path.endswith(ext) for ext in self.HIGH_PRIORITY_FILES
            )
            if is_high_priority:
                prioritized_files.append(file_path)
            else:
                other_files.append(file_path)
        
        # Scan high priority files first
        for file_path in prioritized_files:
            if any(file_path.endswith(ext) for ext in self.SCAN_EXTENSIONS):
                findings = self.scan_content(repo_files[file_path], file_path)
                all_findings.extend(findings)
        
        # Scan other files
        for file_path in other_files:
            if any(file_path.endswith(ext) for ext in self.SCAN_EXTENSIONS):
                findings = self.scan_content(repo_files[file_path], file_path)
                all_findings.extend(findings)
        
        return all_findings
    
    def _mask_secret(self, secret: str) -> str:
        if len(secret) <= 8:
            return '***'
        return secret[:4] + '***' + secret[-4:]
    
    def _get_context(self, line: str, secret: str, context_chars: int = 50) -> str:
        # Find the secret in the line
        index = line.find(secret)
        if index == -1:
            return line[:200]
        
        # Get surrounding context
        start = max(0, index - context_chars)
        end = min(len(line), index + len(secret) + context_chars)
        
        context = line[start:end]
        # Highlight the secret (for display)
        context = context.replace(secret, f'**{secret}**')
        return context
    
    def generate_report(self) -> Dict:
        if not self.findings:
            return {'total': 0, 'by_risk': {}, 'by_type': {}, 'findings': []}
        
        by_risk = {}
        by_type = {}
        
        for finding in self.findings:
            # Count by risk level
            by_risk[finding.risk_level] = by_risk.get(finding.risk_level, 0) + 1
            
            # Count by type
            by_type[finding.type] = by_type.get(finding.type, 0) + 1
        
        return {
            'total': len(self.findings),
            'by_risk': by_risk,
            'by_type': by_type,
            'critical_count': by_risk.get('CRITICAL', 0),
            'high_count': by_risk.get('HIGH', 0),
            'medium_count': by_risk.get('MEDIUM', 0),
            'low_count': by_risk.get('LOW', 0),
            'findings': self.findings
        }
    
    def calculate_risk_score(self) -> float:
        if not self.findings:
            return 0
        
        # Weights for different risk levels
        weights = {
            'CRITICAL': 25,
            'HIGH': 10,
            'MEDIUM': 3,
            'LOW': 1
        }
        
        total_score = 0
        for finding in self.findings:
            total_score += weights.get(finding.risk_level, 1)
        
        # Cap at 100
        return min(total_score, 100)


# Integration with module_code.py
def enhance_code_scanner_with_secrets(code_scanner):
    from module_code import CodeScanner
    
    original_iterative_search = CodeScanner.iterative_search
    
    def iterative_search_with_secrets(self, external_entities=None):
        # Call original method
        result = original_iterative_search(self, external_entities)
        
        # If we have repositories, scan them for secrets
        if not result.repos.empty:
            print(f"\n🔍 Scanning {len(result.repos)} repositories for secrets...")
            secret_detector = SecretDetector()
            
            # This would need actual file content fetching
            # For now, we add a placeholder that triggers on high-risk repos
            # Full implementation would need to fetch file contents via GitHub API
            
            # Add secret findings as a new attribute to the result
            result.secret_findings = []
            
            # Optionally, add a column to repos DataFrame indicating if secrets were found
            result.repos['has_secrets'] = False
            result.repos['secret_count'] = 0
            result.repos['secret_risk'] = 0
        
        return result
    
    # Apply the patch
    CodeScanner.iterative_search = iterative_search_with_secrets
    
    return code_scanner


# Standalone testing function
def test_secret_detector():
    detector = SecretDetector()
    
    sample_code = """
    # AWS credentials
    AWS_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
    AWS_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    
    # GitHub token
    GITHUB_TOKEN = "ghp_123456789012345678901234567890123456"
    
    # Database connection
    DATABASE_URL = "postgresql://user:password@localhost:5432/db"
    
    # Private key
    PRIVATE_KEY = \"\"\"
    -----BEGIN RSA PRIVATE KEY-----
    MIIEpAIBAAKCAQEA...
    -----END RSA PRIVATE KEY-----
    \"\"\"
    
    # Slack webhook
    slack_webhook = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
    
    # Password
    password = "super_secret_password_12345"
    """
    
    findings = detector.scan_content(sample_code, "test_config.py")
    
    print(f"Found {len(findings)} secrets:")
    for finding in findings:
        print(f"  - {finding.type} ({finding.risk_level}): {finding.value}")
        print(f"    File: {finding.file_path}:{finding.line_number}")
        print(f"    Context: {finding.context}\n")
    
    report = detector.generate_report()
    print(f"\nSummary Report:")
    print(f"  Total: {report['total']}")
    print(f"  CRITICAL: {report['critical_count']}")
    print(f"  HIGH: {report['high_count']}")
    print(f"  Risk Score: {detector.calculate_risk_score()}/100")
    
    return detector

if __name__ == "__main__":
    test_secret_detector()