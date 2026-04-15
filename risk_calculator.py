import re
import pandas as pd
from typing import Dict, List, Any, Optional


# Signal weights within each dimension
# Each dimension scores 0–100 independently, then the three are combined.
# Weights below are the contribution of each *signal* within its dimension.

_INFRA_WEIGHTS = {
    "exposed_admin_portal":     30,   # /admin, /phpmyadmin, wp-admin etc. resolved
    "missing_dmarc":            20,   # no DMARC TXT record at all
    "dmarc_policy_none":        10,   # DMARC exists but p=none (monitors only)
    "missing_spf":              15,   # no SPF record
    "spf_too_permissive":        8,   # SPF ends in ~all or +all
    "missing_hsts":             10,   # no Strict-Transport-Security header
    "server_header_exposed":     5,   # Server: Apache/2.4.51 leaks version
    "x_powered_by_exposed":      5,   # X-Powered-By header present
    "safe_browsing_flagged":    40,   # Google Safe Browsing hit
    "per_subdomain":             8,   # each exposed subdomain (capped)
}

_CODE_WEIGHTS = {
    "per_repo_base":             3,   # each public repo adds baseline exposure
    "sensitive_name":           12,   # repo name contains sensitive keyword
    "sensitive_description":     8,   # description contains sensitive keyword
    "high_individual_risk":     15,   # repo.risk_score >= 50
    "medium_individual_risk":    7,   # repo.risk_score 25–49
    "verified_employee_repo":   10,   # repo owner confirmed as employee
    "internal_keyword":         18,   # "internal", "private", "confidential" in name/desc
}

_PEOPLE_WEIGHTS = {
    "per_person_base":           2,   # each discovered identity adds surface area
    "breached_generated_email": 25,   # generated firstname.lastname address in HIBP
    "breached_scraped_email":   20,   # directly scraped email in HIBP
    "social_profile_found":      4,   # confirmed FB/Twitter/Instagram profile
    "github_profile_linked":     3,   # GitHub account linked to employee
    "email_exposed":             6,   # any email address found for the person
    "xing_profile":              1,   # XING profile (lower risk than LinkedIn)
    "large_org_penalty":         8,   # >15 employees discovered (bigger attack surface)
}

# Sensitive keywords checked against repo names and descriptions
_SENSITIVE_REPO_KEYWORDS = [
    "internal", "private", "confidential", "secret", "credential", "cred",
    "password", "passwd", "token", "api-key", "apikey", "auth", "oauth",
    "config", "configuration", "deploy", "deployment", "infrastructure",
    "infra", "prod", "production", "staging", "backup", "dotfiles",
]

_INTERNAL_REPO_KEYWORDS = [
    "internal", "private", "confidential", "intranet", "classified",
]

# Admin portal path patterns that indicate high exposure if subdomains resolve
_ADMIN_PORTAL_PATTERNS = re.compile(
    r"admin|phpmyadmin|wp-admin|cpanel|webmin|plesk|remote|rdp|vpn|"
    r"manage|portal|dashboard|console|control",
    re.IGNORECASE,
)

_RISKY_HEADER_PATTERNS = re.compile(
    r"apache/\d|nginx/\d|php/\d|iis/\d|tomcat/\d|jetty/\d|express/\d",
    re.IGNORECASE,
)


# Dimension scorers

def _score_infrastructure(
    infra_data: List[Dict],
    subdomains: List[Dict],
    safe_search: Dict,
) -> Dict:

    signals: Dict[str, int] = {}
    raw = 0

    # Safe Browsing─
    status = str(safe_search.get("status", "")).lower() if safe_search else ""
    if "❌" in status or "unsafe" in status or "malware" in status:
        signals["safe_browsing_flagged"] = _INFRA_WEIGHTS["safe_browsing_flagged"]
        raw += signals["safe_browsing_flagged"]

    # Subdomains
    if subdomains:
        sub_points = min(len(subdomains) * _INFRA_WEIGHTS["per_subdomain"], 32)
        signals["exposed_subdomains"] = sub_points
        raw += sub_points

        # Admin portal pattern in any subdomain name
        for sub in subdomains:
            portal = str(sub.get("Portal", "") or sub.get("Subdomain", ""))
            if _ADMIN_PORTAL_PATTERNS.search(portal):
                signals["exposed_admin_portal"] = _INFRA_WEIGHTS["exposed_admin_portal"]
                raw += signals["exposed_admin_portal"]
                break  # count once

    # DNS record analysis
    has_spf    = False
    has_dmarc  = False
    spf_strict = False
    dmarc_enforced = False

    for item in (infra_data or []):
        item_str = ""
        if isinstance(item, dict):
            # Flatten all string values for pattern matching
            item_str = " ".join(str(v) for v in item.values()).lower()
        else:
            item_str = str(item).lower()

        # SPF
        if "v=spf1" in item_str:
            has_spf = True
            # -all is strict, ~all is soft-fail (still risky), +all is open relay
            if "-all" in item_str:
                spf_strict = True
            elif "+all" in item_str or "~all" in item_str:
                spf_strict = False  # permissive

        # DMARC
        if "v=dmarc1" in item_str:
            has_dmarc = True
            if "p=reject" in item_str or "p=quarantine" in item_str:
                dmarc_enforced = True

        # HTTP headers — missing HSTS
        if isinstance(item, dict):
            header_type = str(item.get("Type", "") or item.get("Header", "")).lower()
            value       = str(item.get("Value", "") or item.get("Content", "")).lower()

            if "strict-transport-security" in header_type or \
               "strict-transport-security" in item_str:
                pass 
            elif "header" in header_type and "strict" not in item_str:
                # Only penalise once if we see headers but HSTS is absent
                if "missing_hsts" not in signals:
                    signals["missing_hsts"] = _INFRA_WEIGHTS["missing_hsts"]

            # Server version leakage
            server_val = str(item.get("Server", "") or "")
            if _RISKY_HEADER_PATTERNS.search(server_val):
                signals["server_header_exposed"] = _INFRA_WEIGHTS["server_header_exposed"]

            # X-Powered-By
            if "x-powered-by" in item_str or "x_powered_by" in item_str:
                signals["x_powered_by_exposed"] = _INFRA_WEIGHTS["x_powered_by_exposed"]

    if not has_spf:
        signals["missing_spf"] = _INFRA_WEIGHTS["missing_spf"]
    elif not spf_strict:
        signals["spf_too_permissive"] = _INFRA_WEIGHTS["spf_too_permissive"]

    if not has_dmarc:
        signals["missing_dmarc"] = _INFRA_WEIGHTS["missing_dmarc"]
    elif not dmarc_enforced:
        signals["dmarc_policy_none"] = _INFRA_WEIGHTS["dmarc_policy_none"]

    # Add remaining signals to raw
    for k, v in signals.items():
        if k not in ("exposed_subdomains",):
            raw += v

    return {
        "score":   min(round(raw), 100),
        "signals": signals,
    }


def _score_code(code_df: pd.DataFrame) -> Dict:
    if code_df is None or code_df.empty:
        return {"score": 0, "signals": {}}

    signals: Dict[str, int] = {}
    raw = 0

    # Base: each repo adds a small amount of exposure
    base = min(len(code_df) * _CODE_WEIGHTS["per_repo_base"], 18)
    signals["repo_count_base"] = base
    raw += base

    sensitive_hits   = 0
    internal_hits    = 0
    high_risk_count  = 0
    medium_risk_count = 0
    employee_repos   = 0

    for _, repo in code_df.iterrows():
        name  = str(repo.get("repo_name", "") or repo.get("display_name", "")).lower()
        desc  = str(repo.get("description", "")).lower()
        combined = f"{name} {desc}"

        risk_score_val = int(repo.get("risk_score", 0) or 0)

        # Individual repo risk score thresholds
        if risk_score_val >= 50:
            high_risk_count += 1
        elif risk_score_val >= 25:
            medium_risk_count += 1

        # Keyword checks
        if any(kw in combined for kw in _INTERNAL_REPO_KEYWORDS):
            internal_hits += 1
        elif any(kw in combined for kw in _SENSITIVE_REPO_KEYWORDS):
            sensitive_hits += 1

        # Verified employee ownership
        if repo.get("Is_Verified_Employee") is True or \
           repo.get("source") in ("api_contributor", "web_contributor"):
            employee_repos += 1

    if internal_hits:
        pts = min(internal_hits * _CODE_WEIGHTS["internal_keyword"], 36)
        signals["internal_keyword_repos"] = pts
        raw += pts

    if sensitive_hits:
        pts = min(sensitive_hits * _CODE_WEIGHTS["sensitive_name"], 30)
        signals["sensitive_keyword_repos"] = pts
        raw += pts

    if high_risk_count:
        pts = min(high_risk_count * _CODE_WEIGHTS["high_individual_risk"], 30)
        signals["high_risk_repos"] = pts
        raw += pts

    if medium_risk_count:
        pts = min(medium_risk_count * _CODE_WEIGHTS["medium_individual_risk"], 14)
        signals["medium_risk_repos"] = pts
        raw += pts

    if employee_repos:
        pts = min(employee_repos * _CODE_WEIGHTS["verified_employee_repo"], 20)
        signals["employee_linked_repos"] = pts
        raw += pts

    return {
        "score":   min(round(raw), 100),
        "signals": signals,
    }


def _score_people(
    people_df: pd.DataFrame,
    breach_results: Dict,
    generated_breach_results: Dict,
) -> Dict:

    if people_df is None or people_df.empty:
        return {"score": 0, "signals": {}}

    signals: Dict[str, int] = {}
    raw = 0
    total = len(people_df)

    # Surface area base─
    base = min(total * _PEOPLE_WEIGHTS["per_person_base"], 24)
    signals["identity_surface"] = base
    raw += base

    # Large organisation penalty — more employees = bigger attack surface
    if total > 15:
        signals["large_org_penalty"] = _PEOPLE_WEIGHTS["large_org_penalty"]
        raw += signals["large_org_penalty"]

    # Breach signals (highest weight)─
    # Generated addresses (firstname.lastname@domain) found in HIBP
    gen_breached = sum(
        1 for v in (generated_breach_results or {}).values()
        if v.get("status") == "leaked"
    )
    if gen_breached:
        pts = min(gen_breached * _PEOPLE_WEIGHTS["breached_generated_email"], 75)
        signals["generated_email_breaches"] = pts
        raw += pts

    # Directly scraped emails found in HIBP
    scraped_breached = len(breach_results or {})
    if scraped_breached:
        pts = min(scraped_breached * _PEOPLE_WEIGHTS["breached_scraped_email"], 60)
        signals["scraped_email_breaches"] = pts
        raw += pts

    # Email exposure
    emails_found = 0
    social_profiles = 0
    github_linked   = 0

    if "Emails" in people_df.columns:
        emails_found = int(
            people_df["Emails"]
            .apply(lambda x: len(x) if isinstance(x, list) and x else 0)
            .sum()
        )

    if emails_found:
        pts = min(emails_found * _PEOPLE_WEIGHTS["email_exposed"], 24)
        signals["emails_exposed"] = pts
        raw += pts

    # Social profile signals
    if "Gefundene_Links" in people_df.columns:
        for links in people_df["Gefundene_Links"]:
            if not isinstance(links, list):
                continue
            links_str = " ".join(str(l).lower() for l in links)

            # Confirmed social platforms beyond LinkedIn/XING
            if any(p in links_str for p in (
                "facebook.com", "twitter.com", "x.com", "instagram.com"
            )):
                social_profiles += 1

            # GitHub account linked to person
            if "github.com" in links_str:
                github_linked += 1

    if social_profiles:
        pts = min(social_profiles * _PEOPLE_WEIGHTS["social_profile_found"], 20)
        signals["social_profiles_found"] = pts
        raw += pts

    if github_linked:
        pts = min(github_linked * _PEOPLE_WEIGHTS["github_profile_linked"], 12)
        signals["github_profiles_linked"] = pts
        raw += pts

    return {
        "score":   min(round(raw), 100),
        "signals": signals,
    }

# Public API 
def calculate_organization_risk(
    infra_data:               List[Dict]  = None,
    subdomains:               List[Dict]  = None,
    code_df:                  pd.DataFrame = None,
    people_df:                pd.DataFrame = None,
    safe_search:              Dict        = None,
    breach_results:           Dict        = None,
    generated_breach_results: Dict        = None,
) -> Dict:
    
    infra_result  = _score_infrastructure(
        infra_data  or [],
        subdomains  or [],
        safe_search or {},
    )
    code_result   = _score_code(
        code_df if code_df is not None else pd.DataFrame()
    )
    people_result = _score_people(
        people_df if people_df is not None else pd.DataFrame(),
        breach_results           or {},
        generated_breach_results or {},
    )

    infra_score  = infra_result["score"]
    code_score   = code_result["score"]
    people_score = people_result["score"]

    # Weighted combination
    # People weighted highest breached credentials are the most actionable
    # finding and directly map to real compromise risk.
    # Infrastructure second  misconfigs enable phishing and MitM.
    # Code third useful but least directly exploitable without further steps.
    total = (
        infra_score  * 0.30 +
        code_score   * 0.25 +
        people_score * 0.45
    )
    total = min(round(total, 1), 100)

    if   total >= 75: label = "CRITICAL"
    elif total >= 50: label = "HIGH"
    elif total >= 25: label = "MEDIUM"
    else:             label = "LOW"

    return {
        "score": total,
        "label": label,
        "breakdown": {
            "Infrastructure": infra_score,
            "Code":           code_score,
            "People":         people_score,
        },
        "signals": {
            "infrastructure": infra_result["signals"],
            "code":           code_result["signals"],
            "people":         people_result["signals"],
        },
    }