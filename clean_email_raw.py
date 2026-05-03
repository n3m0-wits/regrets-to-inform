import email
from email import policy
from bs4 import BeautifulSoup
import re
import os
import json
import codecs
import unicodedata
import time
def decode_unicode_escapes(text):
    if not text:
        return ""
    
    try:
        if '\\u' in text:
            text = codecs.decode(text, 'unicode_escape')
    except Exception:
        pass
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')

def nuke_invisible_whitespace(text):
    if not text:
        return ""
    # Strip zero-width spaces, non-breaking spaces, and other invisible characters
    text = re.sub(r'[\u200b-\u200f\u2028\u202a-\u202e\u2060-\u206f\ufeff]', '', text)
    # Replace all \xa0 (non-breaking space) with normal space
    text = text.replace('\xa0', ' ')
    # Normalize multiple normal spaces into one
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()

def remove_long_urls(input_text):
    if not input_text:
        return ""
    url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
    return re.sub(url_pattern, "", input_text)

def clean_html_to_text(html_content):
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, 'html.parser')
    # Remove script and style (CSS) elements
    for element in soup(["script", "style", "a"]): # Removes links too
        element.decompose()
        
    # Get text using newline separator to preserve spacing and returns
    text = soup.get_text(separator='\n', strip=True)
    
    # Clean up the output string formatting
    text = decode_unicode_escapes(text)
    text = nuke_invisible_whitespace(text)
    
    # Fix multiple new lines to max two
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    
    return text.strip()

def process_emails(input_dir, output_dir):
    if not os.path.exists(input_dir):
        print(f"Directory '{input_dir}' does not exist.")
        return

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Processing emails in '{input_dir}'... saving to '{output_dir}'")
    
    processed_count = 0

    for filename in os.listdir(input_dir):
        filepath = os.path.join(input_dir, filename)
        
       # Make sure it's a file
        if not os.path.isfile(filepath):
            continue
            
        # READ IN BINARY MODE ('rb')
        with open(filepath, 'rb') as file:
            msg = email.message_from_binary_file(file, policy=policy.default)
        
        # Extract headers
        email_to = msg.get('To', '')
        email_from = msg.get('From', '')
        email_subject = msg.get('Subject', '')
        if email_subject:
            email_subject = decode_unicode_escapes(str(email_subject))
            email_subject = re.sub(r'[—\-]{3,}', '', email_subject)
            email_subject = nuke_invisible_whitespace(email_subject)
        email_cc = msg.get('Cc', '')
        email_date = msg.get('Date', '')
        
        # Extract and clean body
        body_text = ""
        body_part = msg.get_body(preferencelist=('plain', 'html'))
        if body_part:
            text_content = body_part.get_content()
            body_text = remove_long_urls(clean_html_to_text(text_content))
            body_text = re.sub(r'[—\-]{3,}', ' ', body_text)
            
        # Create a dictionary for this email
        email_data = {
            "filename": filename,
            "to": email_to,
            "from": email_from,
            "subject": email_subject,
            "cc": email_cc,
            "date": email_date,
            "body": body_text
        }
        
        # Derive unique filename using email's received date and sender
        sender_raw = str(email_from).strip() if email_from else 'unknown'
        sender_safe = re.sub(r'[^a-zA-Z0-9]', '_', sender_raw)[:40]
        
        date_str = str(email_date) if email_date else ''
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_str)
            time_prefix = str(int(dt.timestamp() * 1000))
        except Exception:
            time_prefix = str(int(time.time() * 1000))

        new_base_name = f"{time_prefix}_{sender_safe}" if sender_safe else time_prefix
        
        # Write each to its own file in the output directory
        output_filepath = os.path.join(output_dir, f"{new_base_name}.json")
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(email_data, f, indent=4)
            
        processed_count += 1

    print(f"Successfully processed {processed_count} emails.")

if __name__ == "__main__":
    process_emails('mime_raw_files', 'output_files')