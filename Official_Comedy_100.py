# Import necessary libraries (Chrome + undetected-chromedriver to reduce Cloudflare/captcha blocks)
import time
import random
import signal
import sys
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import pandas as pd
import re
import csv
import locale
import os
import platform
from typing import Dict, List, Optional, Tuple
import unicodedata
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json

# Silence undetected_chromedriver's noisy __del__ that logs WinError 6 on shutdown
try:
    uc.Chrome.__del__ = lambda self: None
except Exception:
    pass

# Global variable to store the current scraper for cleanup
current_scraper = None

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    print_to_csv("\n⚠️ Received interrupt signal. Cleaning up...")
    if current_scraper is not None:
        try:
            current_scraper.driver.quit()
            print_to_csv("Scraper cleaned up successfully")
        except:
            pass
    print_to_csv("Exiting gracefully...")
    sys.exit(0)

# Set up signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)

# Detect operating system and set appropriate paths
def get_os_specific_paths():
    """Return OS-specific file paths."""
    system = platform.system()
    
    if system == "Windows":
        # Windows paths
        base_dir = r'C:\Users\bigba\aa Personal Projects\Letterboxd-List-Scraping'
        output_dir = os.path.join(base_dir, 'Outputs')
    elif system == "Darwin":  # macOS
        # macOS paths
        base_dir = '/Users/calebcollins/Documents/Letterboxd List Scraping'
        output_dir = os.path.join(base_dir, 'Outputs')
    
    return {
        'base_dir': base_dir,
        'output_dir': output_dir
    }

# Get OS-specific paths
paths = get_os_specific_paths()
BASE_DIR = paths['output_dir']
LIST_DIR = paths['base_dir']

# Optional: Chrome user data dir if you want to reuse a profile (e.g. already logged into Letterboxd).
# Leave None to use a fresh profile each run. Close any open Chrome using that profile before running.
CHROME_USER_DATA_DIR = None  # e.g. r'C:\Users\bigba\AppData\Local\Google\Chrome\User Data'
CHROME_PROFILE_DIR = None    # e.g. 'Default' or 'Profile 1'

# Define a custom print function
def print_to_csv(message: str):
    """Prints a message to the terminal and appends it to All_Outputs.csv."""
    print(message)  # Print to terminal
    with open(os.path.join(BASE_DIR, 'All_Outputs.csv'), mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([message])  # Write the message as a new row

# Configure locale and constants
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
MAX_MOVIES = 125

# Configure settings
MIN_RATING_COUNT = 1000
EXPECTED_LISTING_POSTERS_PER_PAGE = 72


def rating_count_from_page_source(page_source: str) -> int:
    """Letterboxd embeds ratingCount in page source; 0 if missing."""
    m = re.search(r'ratingCount":(\d+)', page_source)
    return int(m.group(1)) if m else 0


def extract_rating_count_from_film_page(driver) -> Optional[int]:
    """Rating count from loaded film HTML only (same pattern as 5000 / Genre 250s). None if absent."""
    try:
        m = re.search(r'ratingCount":(\d+)', driver.page_source)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


# File paths (comedy-specific lists in project root)
BLACKLIST_PATH = os.path.join(LIST_DIR, 'Comedy_Blacklist.xlsx')
WHITELIST_PATH = os.path.join(LIST_DIR, 'Comedy_Whitelist.xlsx')

# Comedy_Whitelist.xlsx: Title, Year, tmdbId, Link.
_WHITELIST_COLUMNS = ['Title', 'Year', 'tmdbId', 'Link']


def normalize_text(text):
    return unicodedata.normalize('NFKC', str(text)).strip()


def masthead_title_from_driver(driver) -> Optional[str]:
    """
    Canonical film title from the Letterboxd film page masthead (h1.primaryname).
    Prefer this over URL slugs / browse-list attributes, which can include year or variant text.
    """
    try:
        h1 = driver.find_element(
            By.CSS_SELECTOR,
            "section.production-masthead h1.headline-1.primaryname",
        )
        try:
            raw = h1.find_element(By.CSS_SELECTOR, "span.name").text
        except Exception:
            raw = h1.text
        if not raw or not str(raw).strip():
            return None
        return normalize_text(str(raw).replace("\xa0", " "))
    except Exception:
        return None


def letterboxd_film_url_key(film_url: Optional[str]) -> str:
    """
    Normalize a Letterboxd film URL (or path) into a stable key for matching.
    Uses the `/film/<slug>/` portion so differences like scheme, trailing slash,
    casing, or query params don't break whitelist/blacklist matching.
    """
    if not film_url:
        return ""
    u = str(film_url).strip()
    if not u:
        return ""
    u = u.split("?", 1)[0].rstrip("/").lower()
    if "/film/" in u:
        return u.split("/film/", 1)[1].strip("/")
    if u.startswith("film/"):
        return u.split("film/", 1)[1].strip("/")
    if u.startswith("/film/"):
        return u.split("/film/", 1)[1].strip("/")
    return u


class MovieProcessor:
    def __init__(self):
        self.whitelist = None
        self.whitelist_lookup = {}
        self.load_whitelist()
        
        # Update blacklist loading to include the Link column
        try:
            if os.path.exists(BLACKLIST_PATH):
                # Try to read without specifying names first to see the actual structure
                temp_df = pd.read_excel(BLACKLIST_PATH, header=0)
                
                # Check if we have the expected columns
                if 'Link' in temp_df.columns and 'Title' in temp_df.columns:
                    # File has the right structure, use it
                    self.blacklist = temp_df
                else:
                    # File exists but has wrong structure, create new one
                    print_to_csv("Blacklist file has wrong structure. Creating new file.")
                    self.blacklist = pd.DataFrame(columns=['Title', 'Year', 'Blank', 'Link'])
                    self.blacklist.to_excel(BLACKLIST_PATH, index=False)
            else:
                print_to_csv("Comedy_Blacklist.xlsx not found. Creating new file.")
                self.blacklist = pd.DataFrame(columns=['Title', 'Year', 'Blank', 'Link'])
                self.blacklist.to_excel(BLACKLIST_PATH, index=False)
                
        except Exception as e:
            print_to_csv(f"Error loading blacklist: {str(e)}")
            print_to_csv("Creating new blacklist file.")
            self.blacklist = pd.DataFrame(columns=['Title', 'Year', 'Blank', 'Link'])
            self.blacklist.to_excel(BLACKLIST_PATH, index=False)
        
        # Normalize titles and years in blacklist
        self.blacklist['Title'] = self.blacklist['Title'].apply(normalize_text)
        self.blacklist['Year'] = self.blacklist['Year'].astype(str).str.strip()
        # Fill empty links with empty string instead of None
        self.blacklist['Link'] = self.blacklist['Link'].fillna('')
        
        # Create a lookup dictionary for faster matching using URLs as keys
        self.blacklist_lookup = {}
        for idx, row in self.blacklist.iterrows():
            if row['Link']:  # Only store entries with URLs
                self.blacklist_lookup[row['Link']] = True
        
        self.rejected_data: List[List] = []

    def load_whitelist(self):
        """Load and initialize the whitelist data."""
        try:
            # First check if file exists
            if not os.path.exists(WHITELIST_PATH):
                print_to_csv("Comedy_Whitelist.xlsx not found. Creating new file.")
                self.whitelist = pd.DataFrame(columns=_WHITELIST_COLUMNS)
                self.whitelist.to_excel(WHITELIST_PATH, index=False)
                return
            
            # Read the file to see what columns it actually has
            try:
                # Try to read without specifying names first to see the actual structure
                temp_df = pd.read_excel(WHITELIST_PATH, header=0)
                
                # Check if we have the expected columns
                if 'Link' in temp_df.columns and 'Title' in temp_df.columns:
                    # File has the right structure, read it normally
                    self.whitelist = temp_df
                else:
                    print_to_csv(
                        f"Whitelist file has unexpected columns {list(temp_df.columns)}; "
                        "need at least Title and Link. Not modifying the file on disk; in-memory whitelist is empty."
                    )
                    self.whitelist = pd.DataFrame(columns=_WHITELIST_COLUMNS)
                    self.whitelist_lookup = {}
                    return
                    
            except Exception as e:
                print_to_csv(f"Error reading whitelist file: {str(e)}")
                print_to_csv("Comedy_Whitelist.xlsx was not overwritten. In-memory whitelist is empty until the file is readable.")
                self.whitelist = pd.DataFrame(columns=_WHITELIST_COLUMNS)
                self.whitelist_lookup = {}
                return

            had_information_column = 'Information' in self.whitelist.columns
            if had_information_column:
                self.whitelist = self.whitelist.drop(columns=['Information'])
            # Whitelist uses tmdbId (never Blank).
            if 'tmdbId' not in self.whitelist.columns:
                self.whitelist['tmdbId'] = ''
            if 'Year' not in self.whitelist.columns:
                self.whitelist['Year'] = ''
            
            # Normalize the data
            self.whitelist['Title'] = self.whitelist['Title'].apply(normalize_text)
            self.whitelist['Year'] = self.whitelist['Year'].astype(str).str.strip()
            # Fill empty links with empty string instead of None
            self.whitelist['Link'] = self.whitelist['Link'].fillna('')
            # Keep tmdbId as-is (user-controlled TMDB id), but normalize NaN to ''
            self.whitelist['tmdbId'] = self.whitelist['tmdbId'].fillna('')
            
            # Create a lookup dictionary for faster matching using URLs as keys.
            # Whitelist is authoritative: we do not need to click into the film page for basic output fields.
            self.whitelist_lookup = {}
            for idx, row in self.whitelist.iterrows():
                raw_link = row.get('Link', '')
                raw_link = '' if pd.isna(raw_link) else str(raw_link).strip()
                if raw_link:
                    info = {
                        'Title': row['Title'] if not pd.isna(row['Title']) else '',
                        'Year': str(row['Year']).strip() if not pd.isna(row['Year']) else '',
                        # Whitelist TMDB id comes from tmdbId column only.
                        'tmdbID': str(row.get('tmdbId', '')).strip()
                        if not pd.isna(row.get('tmdbId', ''))
                        else '',
                    }
                    # Store under both raw URL/path and normalized film key for robust matching.
                    self.whitelist_lookup[raw_link] = (info, idx, raw_link)
                    norm_key = letterboxd_film_url_key(raw_link)
                    if norm_key and norm_key not in self.whitelist_lookup:
                        self.whitelist_lookup[norm_key] = (info, idx, raw_link)

            if had_information_column:
                try:
                    self.whitelist.to_excel(WHITELIST_PATH, index=False)
                    print_to_csv("Saved Comedy_Whitelist.xlsx without legacy Information column.")
                except Exception as ex:
                    print_to_csv(f"Could not save whitelist after removing Information column: {ex}")
                
        except Exception as e:
            print_to_csv(f"Unexpected error loading whitelist: {str(e)}")
            print_to_csv(
                "Comedy_Whitelist.xlsx was not overwritten. In-memory whitelist is empty; fix the spreadsheet or column names."
            )
            self.whitelist = pd.DataFrame(columns=_WHITELIST_COLUMNS)
            self.whitelist_lookup = {}

    def process_whitelist_info(self, info: Dict, film_url: str = None):
        """Process information from whitelist and update statistics."""
        if not isinstance(info, dict):
            print_to_csv("❌ Info is not a dictionary, skipping")
            return

        add_to_MAX_MOVIES(info.get('Title'), info.get('Year'), info.get('tmdbID'), film_url, new_entry='')
     
    def update_whitelist(self, film_title: str, release_year: str, movie_data: Dict, film_url: str = None) -> bool:
        """Update whitelist row by URL (Title/Year only on disk; tmdbId is user-maintained). movie_data is in-memory only for callers before reload."""
        if not film_url:
            return False  # Can't update whitelist without URL
            
        try:
            url_key = letterboxd_film_url_key(film_url)
            # Check if URL already exists in whitelist
            for row_idx, row in self.whitelist.iterrows():
                url = row.get('Link', '')
                url = '' if pd.isna(url) else str(url).strip()
                if url and (url == film_url or letterboxd_film_url_key(url) == url_key):
                    self.whitelist.at[row_idx, 'Title'] = normalize_text(str(film_title))
                    self.whitelist.at[row_idx, 'Year'] = str(release_year).strip() if release_year is not None else ''
                    # Never touch tmdbId here; user maintains tmdbID manually.
                    self.whitelist_lookup[film_url] = (movie_data, row_idx, film_url)
                    if url_key:
                        self.whitelist_lookup[url_key] = (movie_data, row_idx, film_url)
                    # Save to Excel
                    self.whitelist.to_excel(WHITELIST_PATH, index=False)
                    self.load_whitelist()  # Reload to ensure consistency
                    return True
            
            # Add new entry if URL not found
            new_row = pd.DataFrame([{
                'Title': normalize_text(str(film_title)),
                'Year': str(release_year).strip() if release_year is not None else '',
                'tmdbId': '',  # tmdbID (user can fill later)
                'Link': film_url
            }])
            self.whitelist = pd.concat([self.whitelist, new_row], ignore_index=True)
            self.whitelist_lookup[film_url] = (movie_data, len(self.whitelist) - 1, film_url)
            if url_key:
                self.whitelist_lookup[url_key] = (movie_data, len(self.whitelist) - 1, film_url)
            print_to_csv(f"🔗 Added link to whitelist for {film_title}")
            
            # Save to Excel
            self.whitelist.to_excel(WHITELIST_PATH, index=False)
            self.load_whitelist()  # Reload to ensure consistency
            return True
            
        except Exception as e:
            print_to_csv(f"Error updating whitelist: {str(e)}")
            return False

    def get_whitelist_data(self, film_title: str, release_year: str = None, film_url: str = None) -> Optional[Tuple[Dict, int]]:
        """Get the whitelist data for a movie if it exists. Only matches by URL."""
        if not film_url:
            return None, None  # Movie not in whitelist
            
        # Check raw URL, then normalized key
        lookup_key = film_url if film_url in self.whitelist_lookup else letterboxd_film_url_key(film_url)
        if lookup_key in self.whitelist_lookup:
            info, row_idx, _ = self.whitelist_lookup[lookup_key]
            try:
                if isinstance(info, str):
                    info = json.loads(info)
                if not isinstance(info, dict):
                    print_to_csv(f"WARNING: Unexpected data type for {film_title}: {type(info)}")
                    return None, None
                return info, row_idx
            except json.JSONDecodeError as e:
                print_to_csv(f"ERROR parsing whitelist data for {film_title}: {str(e)}")
                print_to_csv(f"Raw data: {info}")
                return None, None
            except Exception as e:
                print_to_csv(f"ERROR processing whitelist data for {film_title}: {str(e)}")
                return None, None
                
        return None, None  # Movie not in whitelist

    def add_to_blacklist(self, film_title: str, release_year: str, reason: str, film_url: str = None) -> None:
        """Add a movie to the blacklist if it fails a criteria, including the link if available. Never patch missing links in existing entries."""
        if not film_url or not reason:
            return
            
        # Check if URL already exists in lookup
        if film_url in self.blacklist_lookup:
            return
            
        # Add new entry
        new_row = pd.DataFrame([[film_title, release_year, reason, film_url]],
                               columns=['Title', 'Year', 'Blank', 'Link'])
        self.blacklist = pd.concat([self.blacklist, new_row], ignore_index=True)
        self.blacklist_lookup[film_url] = True
        self.blacklist.to_excel(BLACKLIST_PATH, index=False)
        print_to_csv(f"⚫ {film_title} ({release_year}) added to blacklist {reason}")

    def is_whitelisted(self, film_title: str, release_year: str, film_url: str = None) -> bool:
        """Check if a movie is in the whitelist using ONLY URL as identifier."""
        if not film_url:
            return False
            
        # Only check URL match, never use title/year
        return film_url in self.whitelist_lookup or letterboxd_film_url_key(film_url) in self.whitelist_lookup

    def is_blacklisted(self, film_title: str, release_year: str = None, film_url: str = None, driver = None) -> bool:
        """Check if a movie is blacklisted using URL as primary identifier."""
        if not film_url:
            return False
            
        # Check if URL exists in blacklist lookup
        return film_url in self.blacklist_lookup

def setup_webdriver():
    """Create Chrome driver using undetected-chromedriver to avoid Cloudflare/captcha detection."""
    def _detect_chrome_major_version() -> Optional[int]:
        """
        Best-effort detection of installed Chrome major version on Windows.
        If detection fails, return None and let undetected-chromedriver decide.
        """
        # 1) Registry (fast, reliable)
        try:
            import winreg  # type: ignore

            for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                for subkey in (
                    r"Software\Google\Chrome\BLBeacon",
                    r"Software\WOW6432Node\Google\Chrome\BLBeacon",
                ):
                    try:
                        k = winreg.OpenKey(hive, subkey)
                        v, _ = winreg.QueryValueEx(k, "version")
                        if v:
                            major = int(str(v).split(".", 1)[0])
                            return major
                    except Exception:
                        continue
        except Exception:
            pass

        # 2) Shell out to chrome.exe (fallback)
        try:
            import subprocess

            out = subprocess.check_output(["chrome", "--version"], stderr=subprocess.STDOUT, text=True)
            # e.g. "Google Chrome 147.0.7727.102"
            m = re.search(r"(\d+)\.\d+\.\d+\.\d+", out)
            if m:
                return int(m.group(1))
        except Exception:
            pass

        return None

    options = uc.ChromeOptions()
    # Prefer normal window (undetected_chromedriver is already less detectable; headless can still be flagged)
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Optional: use existing Chrome profile for Letterboxd login
    if CHROME_USER_DATA_DIR and os.path.isdir(CHROME_USER_DATA_DIR):
        options.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}")
        if CHROME_PROFILE_DIR:
            options.add_argument(f"--profile-directory={CHROME_PROFILE_DIR}")
    # Download preferences (Chrome uses prefs, set via options where possible)
    prefs = {
        "download.default_directory": os.path.join(BASE_DIR, "downloads"),
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    chrome_major = _detect_chrome_major_version()
    # If Chrome updates faster/slower than uc's cached driver, pin the driver major to the installed browser.
    if chrome_major:
        driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_major)
    else:
        driver = uc.Chrome(options=options, use_subprocess=True)
    return driver

def is_retryable_error(error):
    """Determine if an error should be retried based on error type and message."""
    error_str = str(error).lower()
    error_type = type(error).__name__.lower()
    
    # Non-retryable errors (permanent failures)
    non_retryable_indicators = [
        'keyboardinterrupt',
        'systemexit',
        'memoryerror',
        'permission denied',
        'file not found',
        'invalid url',
        'authentication failed',
        'api key',
        'credentials'
    ]
    
    # Check for non-retryable errors
    for indicator in non_retryable_indicators:
        if indicator in error_str or indicator in error_type:
            return False
    
    # Retryable errors (temporary failures)
    retryable_indicators = [
        'timeout',
        'connection',
        'network',
        'temporary',
        'service unavailable',
        'too many requests',
        'rate limit',
        'server error',
        'gateway',
        'bad gateway',
        'no such element',
        'stale element',
        'webdriver',
        'selenium'
    ]
    
    # Check for retryable errors
    for indicator in retryable_indicators:
        if indicator in error_str or indicator in error_type:
            return True
    
    # Default to retryable for unknown errors
    return True

# Final list output (aaOfficial_Comedy_100.csv): Title, Year, tmdbID, Link, New Entry?
OFFICIAL_COMEDY_FILMS: List[Dict] = []

def add_to_MAX_MOVIES(film_title: str, release_year: str, tmdb_id: str, film_url: str, new_entry: str = '') -> bool:
    """
    Append one row for aaOfficial_Comedy_100.csv.
    new_entry: '' for Comedy_Whitelist (URL) entries, 'Yes' for films not on that whitelist.
    Returns True if added, False if missing URL or at MAX_MOVIES.
    Duplicate URL checks happen in process_movie_data / scrape loop.
    """
    global OFFICIAL_COMEDY_FILMS

    if not film_url:
        return False

    if len(OFFICIAL_COMEDY_FILMS) >= MAX_MOVIES:
        return False

    OFFICIAL_COMEDY_FILMS.append({
        'Title': film_title,
        'Year': release_year,
        'tmdbID': tmdb_id,
        'Link': film_url,
        'New Entry?': new_entry,
    })
    return True

class LetterboxdScraper:
    def __init__(self):
        self.driver = setup_webdriver()
        self.processor = MovieProcessor()
        self.base_url = 'https://letterboxd.com/films/by/rating/'
        self.valid_movies_count = 0
        self.page_number = 1
        self.start_time = time.time()
        self.rejected_movies_count = 0
        self._listing_last_url_prev_page: Optional[str] = None
        print_to_csv("Initialized Letterboxd Scraper.")

    @staticmethod
    def _normalize_listing_film_url(film_url: Optional[str]) -> str:
        # Backwards-compat wrapper for shared normalizer.
        return letterboxd_film_url_key(film_url)

    def _build_film_data_list_from_containers(self, film_containers) -> List[dict]:
        film_data_list: List[dict] = []
        for container in film_containers:
            try:
                anchor = container.find_element(By.CSS_SELECTOR, 'a[href*="/film/"]')
                film_url = anchor.get_attribute('href')
                film_title = None
                film_title = container.get_attribute('data-item-full-display-name')
                if not film_title:
                    film_title = container.get_attribute('data-item-name')
                    if film_title:
                        full_name = container.get_attribute('data-item-full-display-name')
                        if full_name and '(' in full_name and ')' in full_name:
                            film_title = full_name
                if not film_title:
                    anchor_title = anchor.get_attribute('title')
                    if anchor_title:
                        title_parts = anchor_title.split(' ')
                        if len(title_parts) > 1 and title_parts[-1].replace('.', '').replace(',', '').isdigit():
                            film_title = ' '.join(title_parts[:-1])
                        else:
                            film_title = anchor_title
                if not film_title:
                    try:
                        img = container.find_element(By.CSS_SELECTOR, 'img')
                        img_alt = img.get_attribute('alt')
                        if img_alt and 'poster' not in img_alt.lower():
                            film_title = img_alt.replace(' poster', '').strip()
                    except Exception:
                        pass
                if not film_title and film_url:
                    url_parts = film_url.split('/film/')
                    if len(url_parts) > 1:
                        title_from_url = url_parts[1].rstrip('/')
                        film_title = title_from_url.replace('-', ' ').replace('_', ' ').title()
                if film_title and film_url:
                    film_title = film_title.strip()
                    release_year = None
                    if '(' in film_title and ')' in film_title:
                        release_year = film_title.split('(')[-1].split(')')[0].strip()
                    is_blacklisted = self.processor.is_blacklisted(None, None, film_url, None)
                    film_data_list.append({
                        'title': film_title,
                        'url': film_url,
                        'is_blacklisted': is_blacklisted,
                        'release_year': release_year,
                    })
                else:
                    print_to_csv(f"Missing data for movie - Title: {film_title}, URL: {film_url}")
                    try:
                        debug_info = (
                            f"Available data: data-item-full-display-name='{container.get_attribute('data-item-full-display-name')}', "
                            f"data-item-name='{container.get_attribute('data-item-name')}', anchor-title='{anchor.get_attribute('title')}'"
                        )
                        print_to_csv(f"   Debug: {debug_info}")
                    except Exception:
                        pass
                    self.processor.rejected_data.append([film_title, None, None, 'Missing title or URL'])
            except Exception as e:
                print_to_csv(f"Error collecting film data: {str(e)}")
                continue
        return film_data_list

    def _process_one_comedy_listing_film(self, film_data: dict) -> bool:
        """Returns True if the outer scrape should stop (MAX_MOVIES reached)."""
        if self.valid_movies_count >= MAX_MOVIES:
            print_to_csv(f"\nReached the target of {MAX_MOVIES} successful movies. Stopping scraping.")
            return True
        film_title = film_data['title']
        film_url = film_data['url']
        release_year = film_data['release_year']
        if film_data['is_blacklisted']:
            print_to_csv(f"❌ {film_title} was not added due to being blacklisted.")
            self.processor.rejected_data.append([film_title, release_year, None, 'Blacklisted'])
            self.rejected_movies_count += 1
            return False
        if any(movie['Link'] == film_url for movie in OFFICIAL_COMEDY_FILMS):
            print_to_csv(f"⚠️ {film_title} was already processed in this session. Skipping.")
            return False
        if self.processor.is_whitelisted(None, None, film_url):
            whitelist_info, _ = self.processor.get_whitelist_data(None, None, film_url)
            self.process_movie_data(whitelist_info or {}, film_title, film_url)
            return False
        movie_retries = 20
        for retry in range(movie_retries):
            try:
                self.driver.get(film_url)
                try:
                    page_title = self.driver.title
                    if "not found" in page_title.lower() or "error" in page_title.lower():
                        print_to_csv(f"⚠️ Movie page appears to be an error page: {page_title}")
                        break
                except Exception:
                    pass
                page_source = self.driver.page_source
                rating_quick = extract_rating_count_from_film_page(self.driver)
                if rating_quick is not None:
                    rating_count = rating_quick
                else:
                    rating_count = rating_count_from_page_source(page_source)
                if rating_count < MIN_RATING_COUNT:
                    reason = (
                        'Zero ratings' if rating_count == 0
                        else f'Insufficient ratings (< {MIN_RATING_COUNT})'
                    )
                    print_to_csv(f"❌ {film_title} was not added: {reason} ({rating_count} ratings).")
                    self.processor.rejected_data.append([film_title, release_year, None, reason])
                    self.rejected_movies_count += 1
                    break
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property=\"og:title\"]'))
                )
                meta_tag = self.driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
                release_year = None
                if meta_tag:
                    release_year_content = meta_tag.get_attribute('content')
                    release_year = release_year_content.split('(')[-1].strip(')')
                masthead_title = masthead_title_from_driver(self.driver)
                display_title = masthead_title if masthead_title else film_title
                tmdb_id = None
                try:
                    tmdb_match = re.search(r'data-tmdb-id="(\d+)"', page_source)
                    if tmdb_match:
                        tmdb_id = tmdb_match.group(1)
                except Exception as e:
                    print_to_csv(f"Error extracting TMDB ID: {str(e)}")
                try:
                    decade = (int(release_year) // 10) * 10 if release_year and str(release_year).isdigit() else None
                except (ValueError, TypeError):
                    decade = None
                movie_data = {
                    'Title': display_title,
                    'Year': release_year,
                    'tmdbID': tmdb_id,
                    'Runtime': None,
                    'RatingCount': rating_count,
                    'Languages': [],
                    'Countries': [],
                    'Decade': decade,
                    'Directors': [],
                    'Genres': [],
                    'Studios': [],
                    'Actors': [],
                    'Link': film_url,
                }
                self.process_movie_data(movie_data, display_title, film_url)
                break
            except Exception as e:
                if retry == movie_retries - 1:
                    print_to_csv(f"❌ Failed to process movie after {movie_retries} attempts: {str(e)}")
                    self.processor.rejected_data.append([film_title, release_year, None, f'Error: {str(e)}'])
                    self.rejected_movies_count += 1
                    break
                else:
                    print_to_csv(f"Retry {retry + 1}/{movie_retries} processing movie: {str(e)}")
                    time.sleep(2)
                    continue
        if self.valid_movies_count >= MAX_MOVIES:
            return True
        return False

    def _maybe_heal_comedy_listing_page_boundary(
        self, film_data_list: List[dict], page_num: int, listing_url: str
    ) -> Tuple[List[dict], bool]:
        """Returns (rows for current page, stop_entire_scrape)."""
        if page_num <= 1 or not film_data_list or not self._listing_last_url_prev_page:
            return film_data_list, False
        if self._normalize_listing_film_url(film_data_list[0]['url']) != self._normalize_listing_film_url(
            self._listing_last_url_prev_page
        ):
            return film_data_list, False
        print_to_csv(
            f"🔗 Listing boundary overlap on page {page_num}: reloading pages {page_num - 1} and {page_num} "
            f"(volatile sort / pagination)."
        )
        prev_url = f'{self.base_url}page/{page_num - 1}/'
        container_retries = 25
        prev_list: List[dict] = []
        for retry in range(container_retries):
            try:
                self.driver.get(prev_url)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'li.posteritem'))
                )
                prev_containers = self.driver.find_elements(By.CSS_SELECTOR, 'li.posteritem')
                n = len(prev_containers)
                if n == EXPECTED_LISTING_POSTERS_PER_PAGE:
                    prev_list = self._build_film_data_list_from_containers(prev_containers)
                    break
                print_to_csv(
                    f"⚠️ Boundary heal prev page: expected {EXPECTED_LISTING_POSTERS_PER_PAGE} posters, found {n}; "
                    f"retry {retry + 1}/{container_retries}"
                )
                time.sleep(3)
                self.driver.get(prev_url)
                time.sleep(2)
            except Exception as e:
                print_to_csv(f"⚠️ Boundary heal load prev page: {e}")
                time.sleep(3)
        if len(prev_list) == 0:
            print_to_csv("⚠️ Boundary heal: could not reload previous page; using original listing.")
            return film_data_list, False
        for fd in prev_list:
            if self._process_one_comedy_listing_film(fd):
                return film_data_list, True
        if prev_list:
            self._listing_last_url_prev_page = prev_list[-1]['url']
        refreshed: List[dict] = []
        for retry in range(container_retries):
            try:
                self.driver.get(listing_url)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'li.posteritem'))
                )
                cur_containers = self.driver.find_elements(By.CSS_SELECTOR, 'li.posteritem')
                n = len(cur_containers)
                if n == EXPECTED_LISTING_POSTERS_PER_PAGE:
                    refreshed = self._build_film_data_list_from_containers(cur_containers)
                    break
                print_to_csv(
                    f"⚠️ Boundary heal current page: expected {EXPECTED_LISTING_POSTERS_PER_PAGE} posters, found {n}; "
                    f"retry {retry + 1}/{container_retries}"
                )
                time.sleep(3)
                self.driver.get(listing_url)
                time.sleep(2)
            except Exception as e:
                print_to_csv(f"⚠️ Boundary heal load current page: {e}")
                time.sleep(3)
        if not refreshed:
            print_to_csv("⚠️ Boundary heal: could not reload current page; using original listing.")
            return film_data_list, False
        return refreshed, False

    def process_movie_data(self, info, film_title=None, film_url=None):
        """Process a film page. Whitelist path: URL in Comedy_Whitelist only; other sheet columns are not used for eligibility."""
        try:
            if not film_url:
                return False
            if info is None:
                info = {}

            film_title = info.get('Title') or film_title
            if isinstance(film_title, str):
                film_title = film_title.strip()
            release_year = info.get('Year')
            tmdb_id = info.get('tmdbID')
            
            # Check if URL has already been processed in this scrape session
            if any(movie['Link'] == film_url for movie in OFFICIAL_COMEDY_FILMS):
                print_to_csv(f"⚠️ {film_title} was already processed in this session. Skipping.")
                return False
                        
            # Whitelist membership: URL on Comedy_Whitelist → add directly without opening the film page.
            # (The whitelist is treated like an allow-list for the output list.)
            if self.processor.is_whitelisted(None, None, film_url):
                self.processor.process_whitelist_info(info, film_url)
                self.valid_movies_count += 1
                print_to_csv(f"✅ Added whitelisted film {film_title} ({self.valid_movies_count}/{MAX_MOVIES})")
                
                # 2% chance to clear the whitelist data for random auditing
                if random.random() < 0.02:
                    self.processor.update_whitelist(film_title, release_year, {}, film_url)
                    print_to_csv(f"🤓 Random data audit scheduled for {film_title} ({release_year})")
                
                return True
            
            # If not whitelisted, process as a new movie
            self.process_approved_movie(film_title, release_year, tmdb_id, film_url, 'scrape')
            return True
                
        except Exception as e:
            print_to_csv(f"Error processing movie data: {str(e)}")
            print_to_csv(f"Error type: {type(e)}")
            print_to_csv(f"Error details: {e.__dict__ if hasattr(e, '__dict__') else 'No details available'}")
            return False

    def scrape_movies(self):
        while self.valid_movies_count < MAX_MOVIES:
            # Safety check: if we've tried too many pages without success, save and exit
            if self.page_number > 1000:  # Arbitrary high limit
                print_to_csv(f"⚠️ Reached page {self.page_number}, which seems too high. Saving progress and stopping.")
                self.save_results_emergency()
                break
                
            # Construct the URL for the current page
            url = f'{self.base_url}page/{self.page_number}/'
            print_to_csv(f"\nLoading page {self.page_number}: {url}")
            
            # Send a GET request to the URL with retry mechanism
            page_retries = 20
            for retry in range(page_retries):
                try:
                    self.driver.get(url)
                    
                    # Check if page loaded successfully
                    try:
                        page_title = self.driver.title
                        
                        # Check if we got redirected to an error page
                        if "not found" in page_title.lower() or "error" in page_title.lower():
                            print_to_csv(f"❌ Page {self.page_number} appears to be an error page: {page_title}")
                            self.page_number += 1
                            continue
                            
                    except Exception as e:
                        print_to_csv(f"Warning: Could not get page title: {str(e)}")
                    
                    # Wait for the page to load
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'li.posteritem'))
                    )
                    
                    # Additional check: verify we're on the right page
                    current_url = self.driver.current_url
                    if current_url != url and "page" not in current_url:
                        print_to_csv(f"⚠️ Page redirected from {url} to {current_url}")
                    
                    break
                except Exception as e:
                    if retry == page_retries - 1:
                        print_to_csv(f"❌ Failed to load page after {page_retries} attempts: {str(e)}")
                        # Try to move to next page instead of crashing
                        print_to_csv(f"Moving to next page and continuing...")
                        self.page_number += 1
                        continue
                    print_to_csv(f"Retry {retry + 1}/{page_retries} loading page {self.page_number}: {str(e)}")
                    time.sleep(2)
                    
                    # Additional error handling for network issues
                    if "timeout" in str(e).lower() or "connection" in str(e).lower():
                        print_to_csv(f"⚠️ Network issue detected, waiting longer before retry...")
                        time.sleep(10)  # Wait longer for network issues
            
            #time.sleep(random.uniform(1.0, 1.5))

            film_containers = []
            container_retries = 25
            for retry in range(container_retries):
                try:
                    film_containers = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'li.posteritem'))
                    )
                    n = len(film_containers)
                    print_to_csv(f"Found {n} film containers on attempt {retry + 1}")
                    if n == EXPECTED_LISTING_POSTERS_PER_PAGE:
                        print_to_csv(f"✅ Found {n} containers (full page), proceeding...")
                        break
                    print_to_csv(
                        f"⚠️ Expected exactly {EXPECTED_LISTING_POSTERS_PER_PAGE} posters, found {n}; "
                        f"reloading listing... (Attempt {retry + 1}/{container_retries})"
                    )
                    time.sleep(3)
                    self.driver.get(url)
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'li.posteritem'))
                    )
                    time.sleep(2)
                except Exception as e:
                    if retry == container_retries - 1:
                        print_to_csv(f"❌ Failed to find film containers after {container_retries} attempts: {str(e)}")
                        print_to_csv("Moving to next page and continuing...")
                        film_containers = []
                        break
                    print_to_csv(f"Retry {retry + 1}/{container_retries} finding film containers: {str(e)}")
                    time.sleep(5)
                    self.driver.refresh()
                    time.sleep(2)
                    if "timeout" in str(e).lower():
                        print_to_csv("⚠️ Timeout detected, waiting longer before retry...")
                        time.sleep(10)

            n_posters = len(film_containers)
            if n_posters != EXPECTED_LISTING_POSTERS_PER_PAGE:
                print_to_csv(
                    f"❌ After retries, expected exactly {EXPECTED_LISTING_POSTERS_PER_PAGE} posters, "
                    f"found {n_posters}. Moving to next page..."
                )
                self.page_number += 1
                continue

            print_to_csv(f"\n{f' Page {self.page_number} ':=^100}")

            film_data_list = self._build_film_data_list_from_containers(film_containers)
            print_to_csv(f"Collected {len(film_data_list)} movies from page {self.page_number}")

            if not film_data_list:
                print_to_csv("No valid film data collected. Moving to next page...")
                self.page_number += 1
                continue

            film_data_list, stop = self._maybe_heal_comedy_listing_page_boundary(
                film_data_list, self.page_number, url
            )
            if stop:
                return

            for film_data in film_data_list:
                if self._process_one_comedy_listing_film(film_data):
                    return

            if film_data_list:
                self._listing_last_url_prev_page = film_data_list[-1]['url']

            self.page_number += 1

    def process_approved_movie(self, film_title: str, release_year: str, tmdb_id: str, film_url: str, approval_type: str):
        """Not on Comedy_Whitelist: ≥ MIN_RATING_COUNT ratings (ratingCount) verified on the film page before this call."""
        try:
            if len(OFFICIAL_COMEDY_FILMS) >= MAX_MOVIES:
                print_to_csv(f"⚠️ {film_title} would exceed limit of {MAX_MOVIES}")
                return
            if not add_to_MAX_MOVIES(film_title, release_year, tmdb_id, film_url, new_entry='Yes'):
                return
            self.valid_movies_count += 1
            print_to_csv(f"✅ {film_title} was approved ({self.valid_movies_count}/{MAX_MOVIES})")
        except Exception as e:
            print_to_csv(f"Error processing approved movie {film_title}: {str(e)}")
            self.processor.rejected_data.append([film_title, release_year, None, f'Error processing: {str(e)}'])

    def save_official_comedy_csv(self):
        """Write aaOfficial_Comedy_100.csv: Title, Year, tmdbID, Link, New Entry?"""
        if not OFFICIAL_COMEDY_FILMS:
            return
        df = pd.DataFrame(OFFICIAL_COMEDY_FILMS)
        df = df[['Title', 'Year', 'tmdbID', 'Link', 'New Entry?']]
        output_path = os.path.join(BASE_DIR, 'aaOfficial_Comedy_100.csv')
        df.to_csv(output_path, index=False, encoding='utf-8')

    def save_results(self):
        self.save_official_comedy_csv()

    def reset_official_comedy_films(self):
        global OFFICIAL_COMEDY_FILMS
        OFFICIAL_COMEDY_FILMS = []

    def reset_counters(self):
        """Reset scraper counters for a new run."""
        self.valid_movies_count = 0
        self.page_number = 1
        self.start_time = time.time()
        self.rejected_movies_count = 0
        self._listing_last_url_prev_page = None
        self.processor.rejected_data = []

    def save_results_emergency(self):
        """Persist whatever made it into the top list so far."""
        self.save_official_comedy_csv()

def main():
    """
    Run the Official Comedy 100 scrape (comedy genre, by rating) with retries.
    Retries on retryable errors with backoff; saves partial CSV on hard failure.
    """
    global current_scraper
    genre, sort_type = "comedy", "rating"
    MAX_RETRIES = 10
    retry_count = 0
    success = False

    while retry_count <= MAX_RETRIES and not success:
        scraper = None
        try:
            if retry_count > 0:
                print_to_csv(f"\n{'Retrying comedy scrape':=^100}")
                print_to_csv(f"Attempt {retry_count + 1}/{MAX_RETRIES + 1}")
            else:
                print_to_csv(f"\n{'Starting Official Comedy 100 scrape':=^100}")

            scraper = LetterboxdScraper()
            current_scraper = scraper

            scraper.base_url = f"https://letterboxd.com/films/genre/{genre}/by/{sort_type}/"
            scraper.reset_official_comedy_films()
            scraper.reset_counters()
            scraper.scrape_movies()
            scraper.save_results()

            success = True
            print_to_csv(f"✅ Successfully completed Official Comedy 100")

        except Exception as e:
            retry_count += 1
            print_to_csv(f"\n{'Error':=^100}")
            print_to_csv(f"❌ An error occurred during execution: {e}")
            print_to_csv(f"Error type: {type(e).__name__}")
            print_to_csv(f"Error details: {str(e)}")

            if not is_retryable_error(e):
                print_to_csv("❌ Non-retryable error detected. Stopping retries.")
                if scraper is not None:
                    try:
                        scraper.save_results_emergency()
                        print_to_csv("💾 Emergency results saved")
                    except Exception as save_error:
                        print_to_csv(f"❌ Failed to save emergency results: {save_error}")
                break

            if retry_count <= MAX_RETRIES:
                wait_time = min(30 * retry_count, 120)
                print_to_csv(f"🔄 Will retry (attempt {retry_count + 1}/{MAX_RETRIES + 1})")
                print_to_csv(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print_to_csv(f"❌ Failed after {MAX_RETRIES + 1} attempts.")
                if scraper is not None:
                    try:
                        scraper.save_results_emergency()
                        print_to_csv("💾 Emergency results saved")
                    except Exception as save_error:
                        print_to_csv(f"❌ Failed to save emergency results: {save_error}")
        finally:
            if scraper is not None:
                try:
                    print_to_csv("Cleaning up scraper...")
                    scraper.driver.quit()
                    print_to_csv("Scraper cleaned up successfully")
                    current_scraper = None
                except Exception as cleanup_error:
                    print_to_csv(f"Error during cleanup: {cleanup_error}")
                    try:
                        scraper.driver.service.process.kill()
                    except Exception:
                        pass
                    current_scraper = None

if __name__ == "__main__":
    main()