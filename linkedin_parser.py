import pandas as pd
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def get_furthest_past_date(time_str):
    if pd.isna(time_str) or not time_str:
        return pd.NaT
        
    time_str = str(time_str).lower()
    current_time = datetime.now()
    
    if 'just now' in time_str:
        return current_time - timedelta(seconds=59)
        
    match = re.search(r'(\d+)\s*(m|h|d|w|mo|yr)', time_str)
    if not match:
        return pd.NaT
        
    val = int(match.group(1)) + 1
    unit = match.group(2)
    
    delta_args = {}
    if unit == 'm':   delta_args = {'minutes': val}
    elif unit == 'h': delta_args = {'hours': val}
    elif unit == 'd': delta_args = {'days': val}
    elif unit == 'w': delta_args = {'weeks': val}
    elif unit == 'mo':delta_args = {'months': val}
    elif unit == 'yr':delta_args = {'years': val}
        
    return current_time - relativedelta(**delta_args) + timedelta(seconds=1)


def parse_jobs(file_path='linkedindump.txt'):
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()

    blocks = re.split(r'Notes\s*Add note', text)
    data = []

    noise_regex = re.compile(
        r'^(0 notifications|Saved .*|In Progress .*|Applied · .*|Interview · .*|'
        r'Archived|Date posted|About archived jobs|Previous|Next|Learn more|'
        r'Only open jobs can be unarchived\.|Saved jobs move to Archived.*|'
        r'Did you hear back\?|YesNo|Yes|No|Not seeing some jobs\?|•\s*\d*)$',
        re.IGNORECASE
    )

    status_regex = re.compile(
        r'^(Applied \d+\w* ago|Applied just now|\(?No longer accepting applications\)?|'
        r'\(?Posted .* ago\)?|\(?Reposted .* ago\)?|Application viewed|Resume downloaded|'
        r'Not moving forward|Interview)$',
        re.IGNORECASE
    )

    for block in blocks:
        lines = [line.strip() for line in block.split('\n') if line.strip()]
        clean_lines = [line for line in lines if not noise_regex.match(line)]
        
        if not clean_lines:
            continue
            
        header_lines = []
        statuses = []
        
        for line in clean_lines:
            if status_regex.match(line):
                statuses.append(line)
            else:
                header_lines.append(line)
                
        if not header_lines:
            continue
            
        role = None
        company_line = None
        
        if len(header_lines) == 1:
            line = header_lines[0]
            if re.search(r'[·•]', line) or any(k in line for k in ['(Remote)', '(Hybrid)', '(On-site)']):
                company_line = line
            else:
                role = line
        elif len(header_lines) >= 2:
            role = header_lines[0]
            company_line = header_lines[1]

        company, location = None, None
        if company_line:
            parts = re.split(r'\s*[·•]\s*', company_line, maxsplit=1)
            
            if len(parts) == 2:
                p1, p2 = parts[0].strip(), parts[1].strip()
                workplace_types = ['(hybrid)', '(on-site)', '(remote)', 'hybrid', 'on-site', 'remote']
                if p2.lower() in workplace_types:
                    company = None
                    location = f"{p1} {p2}"
                else:
                    company = p1
                    location = p2
            else:
                val = parts[0].strip()
                loc_hints = ['remote', 'hybrid', 'on-site', 'metropolitan area']
                if any(h in val.lower() for h in loc_hints):
                    location = val
                else:
                    company = val

        work_type = None
        if location:
            wt_match = re.search(r'\((remote|hybrid|on-site)\)', location, re.IGNORECASE)
            if wt_match:
                work_type = wt_match.group(1).capitalize()
                location = location.replace(wt_match.group(0), '').strip()

        raw_applied = next((s for s in statuses if s.lower().startswith('applied')), None)
        raw_posted = next((s.strip('() ') for s in statuses if 'posted' in s.lower() and 'reposted' not in s.lower()), None)
        raw_reposted = next((s.strip('() ') for s in statuses if 'reposted' in s.lower()), None)
        
        is_open = not any('accepting' in s.lower() for s in statuses)
        
        if not is_open:
            listing_status = 'closed'
        elif raw_reposted:
            listing_status = 'reposted'
        else:
            listing_status = 'open'

        resume_downloaded = any('downloaded' in s.lower() for s in statuses)
        
        app_viewed = any('viewed' in s.lower() for s in statuses) or resume_downloaded
        
        outcome = None
        if any('not moving forward' in s.lower() for s in statuses):
            outcome = 'Not moving forward'
        elif any('interview' in s.lower() for s in statuses):
            outcome = 'Interview'

        data.append({
            'role': role,
            'company': company,
            'location': location,
            'work_type': work_type,
            'is_open': is_open,
            'listing_status': listing_status,
            'app_viewed': app_viewed,
            'resume_downloaded': resume_downloaded,
            'outcome': outcome,
            'raw_applied': raw_applied,
            'raw_posted': raw_posted,
            'raw_reposted': raw_reposted
        })

    df = pd.DataFrame(data)
    
    df['applied_date'] = df['raw_applied'].apply(get_furthest_past_date)
    df['posted_date'] = df['raw_posted'].apply(get_furthest_past_date)
    df['reposted_date'] = df['raw_reposted'].apply(get_furthest_past_date)
    
    cols = [
        'role', 'company', 'location', 'work_type', 'is_open', 'listing_status',
        'raw_applied', 'applied_date', 'raw_posted', 'posted_date', 'raw_reposted', 'reposted_date',
        'app_viewed', 'resume_downloaded', 'outcome'
    ]
    
    return df[cols]