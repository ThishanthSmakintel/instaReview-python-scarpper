import requests
import re
import pandas as pd
import os
import json
from bs4 import BeautifulSoup
import time
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

# ========================
# CONFIGURATION
# ========================
API_KEY = os.getenv("API_KEY")
CSE_ID = os.getenv("CSE_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Configure Gemini
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
QUERY = 'site:.sg "restaurant" ("contact us" OR "contact" OR "email" OR "phone" OR "address")'
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
# VALIDATION FUNCTIONS
# ========================
def validate_config():
    """Validate required configuration"""
    if not API_KEY or not CSE_ID:
        raise ValueError("Missing required API_KEY or CSE_ID in .env file")
    print("[VALIDATION] Configuration validated successfully")

def validate_email(email):
    """Validate email format"""
    if email == "-":
        return True
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def validate_url(url):
    """Validate URL format"""
    if not url or url == "-":
        return False
    return url.startswith(('http://', 'https://'))

def sanitize_filename(filename):
    """Remove dangerous characters from filename"""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

# ========================
# FUNCTIONS
# ========================
def fetch_cse_results(query, start=1):
    """Fetch search results with validation"""
    if not query or len(query.strip()) < 3:
        raise ValueError("Query must be at least 3 characters")
    if start < 1 or start > 100:
        raise ValueError("Start index must be between 1 and 100")
    
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": API_KEY,
        "cx": CSE_ID,
        "q": query,
        "num": min(NUM_RESULTS, 10),  # Max 10 per request
        "start": start
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'error' in data:
            raise Exception(f"API Error: {data['error']['message']}")
            
        print(f"[INFO] Total results available: {data.get('searchInformation', {}).get('totalResults', '0')}")
        return data.get("items", [])
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Network error: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] API error: {e}")
        return []

def extract_emails(text):
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    return emails if emails else ["-"]

def extract_phones(text):
    phones = re.findall(r"\+?\d[\d\s\-]{7,}\d", text)
    return phones if phones else ["-"]

def clean_email(email_str):
    """Clean and validate email addresses"""
    if not email_str or email_str == "-":
        return "-"
    
    emails = email_str.split(", ")
    cleaned = []
    
    for email in emails:
        # Remove extra text and clean
        email = re.sub(r'[^a-zA-Z0-9@._-]', '', email).strip()
        if len(email) > 5 and validate_email(email):
            cleaned.append(email.lower())
    
    # Remove duplicates and return
    unique_emails = list(dict.fromkeys(cleaned))
    return ", ".join(unique_emails) if unique_emails else "-"

def clean_phone(phone_str):
    """Clean and validate phone numbers"""
    if not phone_str or phone_str == "-":
        return "-"
    
    phones = phone_str.split(", ")
    cleaned = []
    
    for phone in phones:
        # Remove whitespace and invalid chars
        phone = re.sub(r'[^\d+\-\s]', '', phone).strip()
        digits_only = re.sub(r'[^\d]', '', phone)
        # Validate phone length (7-15 digits)
        if 7 <= len(digits_only) <= 15:
            cleaned.append(phone)
    
    # Remove duplicates
    unique_phones = list(dict.fromkeys(cleaned))
    return ", ".join(unique_phones) if unique_phones else "-"

def extract_domain_name(url):
    """Extract domain name from URL"""
    try:
        # Remove protocol and www
        domain = re.sub(r'^https?://(www\.)?', '', url)
        # Extract domain between start and first dot or slash
        domain = re.split(r'[./]', domain)[0]
        # Clean domain name
        domain = re.sub(r'[^a-zA-Z0-9]', '', domain)
        return domain.capitalize() if domain else "Unknown"
    except:
        return "Unknown"

def clean_name(name, website=""):
    """Clean name and use domain if name is generic"""
    # Remove common suffixes and clean
    name = re.sub(r'\s*[|\-].*$', '', name)  # Remove everything after | or -
    name = re.sub(r'\s+', ' ', name).strip()  # Normalize spaces
    
    # If name is generic, use domain name
    generic_names = ['contact us', 'contact', 'home', 'enquiries', 'about us', 'enquiry']
    if name.lower() in generic_names and website:
        domain_name = extract_domain_name(website)
        return f"{domain_name} Restaurant"
    
    return name if name else "Unknown Restaurant"

def scrape_website_content(url):
    """Scrape website content with validation and error handling"""
    if not validate_url(url):
        print(f"[WARNING] Invalid URL: {url}")
        return ""
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        response.raise_for_status()
        
        # Check content type
        content_type = response.headers.get('content-type', '').lower()
        if 'text/html' not in content_type:
            print(f"[WARNING] Non-HTML content: {content_type}")
            return ""
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
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
        
        return text_content.strip()
    except requests.exceptions.Timeout:
        print(f"[WARNING] Timeout scraping: {url}")
        return ""
    except requests.exceptions.RequestException as e:
        print(f"[WARNING] Error scraping {url}: {e}")
        return ""
    except Exception as e:
        print(f"[WARNING] Unexpected error scraping {url}: {e}")
        return ""

def load_existing_data():
    """Load existing data with validation"""
    if not os.path.exists(json_path):
        return []
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                print("[WARNING] Invalid data format, starting fresh")
                return []
            print(f"[INFO] Loaded {len(data)} existing restaurants")
            return data
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"[ERROR] Failed to load existing data: {e}")
        return []

def load_state():
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"start_index": 1, "scraped_urls": []}

def save_state(state):
    """Save state with validation"""
    if not isinstance(state, dict):
        raise ValueError("State must be a dictionary")
    
    try:
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        print(f"[ERROR] Failed to save state: {e}")

def update_missing_contacts():
    restaurants = load_existing_data()
    updated_count = 0
    missing_restaurants = []
    
    # First try web scraping
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
            
            # Still missing? Add to Gemini list
            if restaurant['email'] == '-':
                missing_restaurants.append(restaurant)
            
            time.sleep(1)
    
    # Try Gemini for remaining missing contacts
    if missing_restaurants and GEMINI_API_KEY:
        print(f"\n[GEMINI] Trying to find {len(missing_restaurants)} missing contacts...")
        gemini_results = gemini_fallback_bulk(missing_restaurants)
        
        for result in gemini_results:
            for restaurant in restaurants:
                if restaurant['name'] == result['name'] and result.get('email', '-') != '-':
                    restaurant['email'] = result['email']
                    updated_count += 1
                    print(f"[GEMINI FOUND] {restaurant['name']}: {result['email']}")
    
    if updated_count > 0:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(restaurants, f, indent=4)
        print(f"\n[SUCCESS] Updated {updated_count} restaurants with missing emails")
    else:
        print("\n[INFO] No missing emails found to update")

def gemini_fallback_bulk(missing_restaurants):
    """Use Gemini API to find missing contact info"""
    if not missing_restaurants or not GEMINI_API_KEY:
        print("[GEMINI] No API key or restaurants provided")
        return [{"name": r['name'], "website": r['website'], "email": "-", "phone": "-"} for r in missing_restaurants]
    
    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        restaurants_text = "\n".join([f"{r['name']} - {r['website']}" for r in missing_restaurants[:3]])
        prompt = f"Find contact emails for these Singapore restaurants:\n{restaurants_text}\n\nReturn only valid email addresses, one per line. If not found, return '-' for that restaurant."
        
        print(f"[GEMINI] Sending request for {len(missing_restaurants[:3])} restaurants...")
        
        response = model.generate_content(prompt)
        text = response.text
        print(f"[GEMINI] Response: {text[:200]}...")
        
        # Extract emails from response
        emails = extract_emails(text)
        if emails != ["-"]:
            print(f"[GEMINI] Found emails: {emails}")
            # Assign emails to restaurants
            results = []
            email_idx = 0
            for r in missing_restaurants[:3]:
                if email_idx < len(emails) and emails[email_idx] != "-":
                    results.append({"name": r['name'], "website": r['website'], "email": emails[email_idx], "phone": "-"})
                    email_idx += 1
                else:
                    results.append({"name": r['name'], "website": r['website'], "email": "-", "phone": "-"})
            return results
            
    except Exception as e:
        print(f"[GEMINI ERROR] {e}")
    
    return [{"name": r['name'], "website": r['website'], "email": "-", "phone": "-"} for r in missing_restaurants]

# ========================
# MAIN
# ========================
def main():
    """Main execution with validation"""
    try:
        # Validate configuration
        validate_config()
        
        all_restaurants = load_existing_data()
        state = load_state()
        
        print(f"[INFO] Starting from result #{state['start_index']}")
        print(f"[INFO] Trying search query: '{QUERY}'")
        results = fetch_cse_results(QUERY, start=state['start_index'])
        
        if not results:
            print("[WARNING] No results found")
            return
        
        print(f"[SUCCESS] {len(results)} results retrieved for query: '{QUERY}'")
        new_count = 0
        
        for idx, item in enumerate(results, start=state['start_index']):
            if not isinstance(item, dict):
                print(f"[WARNING] Invalid result format at index {idx}")
                continue
                
            title = item.get("title", "-")
            snippet = item.get("snippet", "")
            link = item.get("link", "-")
            
            # Validate URL
            if not validate_url(link):
                print(f"[SKIP {idx}] Invalid URL: {link}")
                continue
            
            # Skip if already scraped
            if link in state['scraped_urls']:
                print(f"[SKIP {idx}] Already scraped: {title}")
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
                "name": clean_name(title, link),
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
        
        # Update state & save
        state['start_index'] += NUM_RESULTS
        save_state(state)
        
        # Save data
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(all_restaurants, f, indent=4)
            print(f"\n[SUCCESS] {new_count} new restaurants added. Total: {len(all_restaurants)}")
            print(f"[SUCCESS] Restaurant data saved as JSON: '{json_path}'")
        except Exception as e:
            print(f"[ERROR] Failed to save JSON: {e}")
            return
        
        # Save to Excel
        try:
            df = pd.DataFrame(all_restaurants)
            df = df.drop_duplicates(subset=['website'], keep='first')
            df.to_excel(excel_path, index=False)
            print(f"[SUCCESS] Data exported to Excel: '{excel_path}'")
            print(f"[INFO] Total unique restaurants: {len(df)}")
        except Exception as e:
            print(f"[ERROR] Failed to save Excel: {e}")
        
        # Auto-update missing contacts
        try:
            print("\n[AUTO] Updating missing emails from existing data...")
            update_missing_contacts()
            
            print("\n[AUTO] Using Gemini AI for remaining missing contacts...")
            restaurants = load_existing_data()
            missing = [r for r in restaurants if r['email'] == '-']
            if missing:
                print(f"[INFO] Found {len(missing)} restaurants with missing emails")
                gemini_results = gemini_fallback_bulk(missing)
                
                updated = 0
                for result in gemini_results:
                    for restaurant in restaurants:
                        if restaurant['name'] == result['name'] and result.get('email', '-') != '-':
                            restaurant['email'] = result['email']
                            if result.get('phone', '-') != '-':
                                restaurant['phone'] = result['phone']
                            updated += 1
                
                if updated > 0:
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(restaurants, f, indent=4)
                    print(f"[SUCCESS] Gemini updated {updated} restaurants")
            else:
                print("[INFO] No missing contacts to update")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
    
    except Exception as e:
        print(f"[FATAL ERROR] {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
