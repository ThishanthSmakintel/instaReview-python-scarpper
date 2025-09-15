import requests
import re
import pandas as pd
import os
import json
from bs4 import BeautifulSoup
import time
from dotenv import load_dotenv

load_dotenv()

# ========================
# CONFIGURATION
# ========================
API_KEY = os.getenv("API_KEY")
CSE_ID = os.getenv("CSE_ID")
QUERY = 'site:.lk "restaurant" ("contact us" OR "contact" OR "email" OR "phone" OR "address")'
NUM_RESULTS = 10  # per request
FOLDER_NAME = "exported_data"
JSON_FILE = "restaurant_details.json"
EXCEL_FILE = "restaurant_emails.xlsx"
STATE_FILE = "scraping_state.json"

# ========================
# CREATE FOLDER IF NOT EXISTS
# ========================
if not os.path.exists(FOLDER_NAME):
    os.makedirs(FOLDER_NAME)

json_path = os.path.join(FOLDER_NAME, JSON_FILE)
excel_path = os.path.join(FOLDER_NAME, EXCEL_FILE)
state_path = os.path.join(FOLDER_NAME, STATE_FILE)

# ========================
# FUNCTIONS
# ========================
def fetch_cse_results(query, start=1):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": API_KEY,
        "cx": CSE_ID,
        "q": query,
        "num": NUM_RESULTS,
        "start": start
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"[INFO] Total results available: {data.get('searchInformation', {}).get('totalResults', '0')}")
    return data.get("items", [])

def extract_emails(text):
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    return emails if emails else ["-"]

def extract_phones(text):
    phones = re.findall(r"\+?\d[\d\s\-]{7,}\d", text)
    return phones if phones else ["-"]

def clean_email(email_str):
    if email_str == "-":
        return "-"
    
    emails = email_str.split(", ")
    cleaned = []
    
    for email in emails:
        # Remove extra text and clean
        email = re.sub(r'[^a-zA-Z0-9@._-]', '', email)
        if '@' in email and '.' in email.split('@')[-1]:
            cleaned.append(email.lower())
    
    # Remove duplicates and return
    unique_emails = list(dict.fromkeys(cleaned))
    return ", ".join(unique_emails) if unique_emails else "-"

def clean_phone(phone_str):
    if phone_str == "-":
        return "-"
    
    phones = phone_str.split(", ")
    cleaned = []
    
    for phone in phones:
        # Remove whitespace and invalid chars
        phone = re.sub(r'[^\d+\-\s]', '', phone).strip()
        # Remove very short or invalid numbers
        if len(re.sub(r'[^\d]', '', phone)) >= 7:
            cleaned.append(phone)
    
    # Remove duplicates
    unique_phones = list(dict.fromkeys(cleaned))
    return ", ".join(unique_phones) if unique_phones else "-"

def clean_name(name):
    # Remove common suffixes and clean
    name = re.sub(r'\s*[|\-].*$', '', name)  # Remove everything after | or -
    name = re.sub(r'\s+', ' ', name).strip()  # Normalize spaces
    return name if name else "Unknown Restaurant"

def scrape_website_content(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Focus on contact-related sections
        contact_sections = soup.find_all(['div', 'section', 'footer'], 
                                       string=re.compile(r'contact|email|phone', re.I))
        
        text_content = ''
        if contact_sections:
            for section in contact_sections[:3]:  # Limit to first 3 matches
                text_content += section.get_text() + ' '
        else:
            # Fallback to full page text (limited)
            text_content = soup.get_text()[:5000]  # First 5000 chars only
        
        return text_content
    except:
        return ""

def load_existing_data():
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def load_state():
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"start_index": 1, "scraped_urls": []}

def save_state(state):
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4)

def update_missing_contacts():
    restaurants = load_existing_data()
    updated_count = 0
    
    for restaurant in restaurants:
        if restaurant['email'] == '-':
            print(f"\n[UPDATE] Checking {restaurant['name']}...")
            website_content = scrape_website_content(restaurant['website'])
            if website_content:
                emails = extract_emails(website_content)
                phones = extract_phones(website_content)
                
                if emails != ['-']:
                    restaurant['email'] = ', '.join(emails)
                    updated_count += 1
                    print(f"[FOUND] Email: {restaurant['email']}")
                
                if restaurant['phone'] == '-' and phones != ['-']:
                    restaurant['phone'] = ', '.join(phones)
                    print(f"[FOUND] Phone: {restaurant['phone']}")
            
            time.sleep(1)
    
    if updated_count > 0:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(restaurants, f, indent=4)
        print(f"\n[SUCCESS] Updated {updated_count} restaurants with missing emails")
    else:
        print("\n[INFO] No missing emails found to update")

# ========================
# MAIN
# ========================
all_restaurants = load_existing_data()
state = load_state()

print(f"[INFO] Starting from result #{state['start_index']}")
print(f"[INFO] Trying search query: '{QUERY}'")
results = fetch_cse_results(QUERY, start=state['start_index'])

print(f"[SUCCESS] {len(results)} results retrieved for query: '{QUERY}'")
new_count = 0

for idx, item in enumerate(results, start=state['start_index']):
    title = item.get("title", "-")
    snippet = item.get("snippet", "")
    link = item.get("link", "-")
    
    # Skip if already scraped
    if link in state['scraped_urls']:
        print(f"\n[SKIP {idx}] Already scraped: {title}")
        continue
    
    emails = extract_emails(snippet)
    phones = extract_phones(snippet)
    
    # If email missing, scrape website
    if emails == ["-"]:
        print(f"[INFO] Scraping website for missing email: {link}")
        website_content = scrape_website_content(link)
        if website_content:
            website_emails = extract_emails(website_content)
            if website_emails != ["-"]:
                emails = website_emails
                print(f"[FOUND] Email from website: {', '.join(emails)}")
        time.sleep(1)  # Be respectful to servers
    
    restaurant_data = {
        "name": clean_name(title),
        "website": link,
        "email": clean_email(", ".join(emails)),
        "phone": clean_phone(", ".join(phones))
    }
    all_restaurants.append(restaurant_data)
    state['scraped_urls'].append(link)
    new_count += 1
    
    # Log each result
    print(f"\n[NEW {idx}]")
    print(f"Title: {title}")
    print(f"Link: {link}")
    print(f"Emails: {restaurant_data['email']}")
    print(f"Phones: {restaurant_data['phone']}")

# ========================
# UPDATE STATE & SAVE
# ========================
state['start_index'] += NUM_RESULTS
save_state(state)

with open(json_path, "w", encoding="utf-8") as f:
    json.dump(all_restaurants, f, indent=4)
print(f"\n[SUCCESS] {new_count} new restaurants added. Total: {len(all_restaurants)}")
print(f"[SUCCESS] Restaurant data saved as JSON: '{json_path}'")

# ========================
# SAVE TO EXCEL
# ========================
df = pd.DataFrame(all_restaurants)
df.drop_duplicates(inplace=True)
df.to_excel(excel_path, index=False)
print(f"[SUCCESS] Excel data saved to folder '{FOLDER_NAME}' as '{EXCEL_FILE}'")

# ========================
# UPDATE MISSING CONTACTS
# ========================
if input("\nUpdate missing emails from existing data? (y/n): ").lower() == 'y':
    update_missing_contacts()

# Clean current data
if input("\nClean existing data? (y/n): ").lower() == 'y':
    restaurants = load_existing_data()
    for restaurant in restaurants:
        restaurant['name'] = clean_name(restaurant['name'])
        restaurant['email'] = clean_email(restaurant['email'])
        restaurant['phone'] = clean_phone(restaurant['phone'])
    
    # Remove duplicates
    seen_websites = set()
    unique_restaurants = []
    for restaurant in restaurants:
        if restaurant['website'] not in seen_websites:
            seen_websites.add(restaurant['website'])
            unique_restaurants.append(restaurant)
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(unique_restaurants, f, indent=4)
    
    print(f"[SUCCESS] Cleaned data and removed {len(restaurants) - len(unique_restaurants)} duplicates")
