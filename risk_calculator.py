import pandas as pd

def calculate_organization_risk(infra_data, subdomains, code_df, people_df):

    # 1. INFRASTRUCTURE RISK (Weight: 0.4)
    infra_score = 0
    
    # Subdomain Takeover Risk (Critical)
    if subdomains and len(subdomains) > 0:
        infra_score += min(len(subdomains) * 10, 40)
        
    # Open Ports / Outdated Tech (Requires analyzing infra_data list)
    if infra_data:
        for item in infra_data:
            # Check for critical exposed technologies or headers
            tech = str(item).lower()
            if any(risk in tech for risk in ['wp-admin', 'phpMyAdmin', 'remote', 'rdp', 'ftp']):
                infra_score += 20
            if 'strict-transport-security' not in tech:  # Missing HSTS
                infra_score += 5

    infra_score = min(infra_score, 100)

    # 2. CODE / GITHUB RISK (Weight: 0.3)
    code_score = 0
    if not code_df.empty:
        # Base risk for having public repos
        code_score += min(len(code_df) * 2, 20)
        
        # Check repo names/descriptions for sensitive keywords
        sensitive_keywords = ['internal', 'api', 'secret', 'auth', 'config', 'credential', 'token']
        for _, repo in code_df.iterrows():
            text_to_check = f"{repo.get('repo_name', '')} {repo.get('description', '')}".lower()
            if any(keyword in text_to_check for keyword in sensitive_keywords):
                code_score += 15
                
    code_score = min(code_score, 100)

    # 3. PEOPLE / SOCIAL RISK (Weight: 0.3)
    people_score = 0
    if not people_df.empty:
        total_people = len(people_df)
        people_score += min(total_people * 2, 30) # Baseline surface area risk
        
        # Check for exposed emails
        if 'Emails' in people_df.columns:
            emails_found = people_df['Emails'].apply(lambda x: len(x) if isinstance(x, list) else 0).sum()
            if total_people > 0:
                exposure_ratio = emails_found / total_people
                people_score += (exposure_ratio * 70) # High penalty if many emails are exposed

    people_score = min(people_score, 100)

    # FINAL CALCULATION
    # Weights: Infra (40%), Code (30%), People (30%)
    total_risk = (infra_score * 0.4) + (code_score * 0.3) + (people_score * 0.3)
    
    if total_risk >= 75: label = "CRITICAL"
    elif total_risk >= 50: label = "HIGH"
    elif total_risk >= 25: label = "MEDIUM"
    else: label = "LOW"

    return {
        "score": round(total_risk, 1),
        "label": label,
        "breakdown": {
            "Infrastructure": round(infra_score, 1),
            "Code": round(code_score, 1),
            "People": round(people_score, 1)
        }
    }