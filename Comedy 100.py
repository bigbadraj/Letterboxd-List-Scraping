import requests
from bs4 import BeautifulSoup
import json
import time
import csv
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException, WebDriverException
import os
import platform
from tqdm import tqdm

# Silence undetected_chromedriver's noisy __del__ that logs WinError 6 on shutdown
try:
    uc.Chrome.__del__ = lambda self: None
except Exception:
    pass

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
output_dir = paths['output_dir']

# Set to True to only run list-page debug (no film page visits, no CSV write)
DEBUG_LIST_PAGE = False
# Set to True to scrape from list only (no per-film page visits for rating count)
LIST_ONLY = True

# Optional: Chrome user data dir if you want to reuse a profile (e.g. already logged into Letterboxd).
# Leave None to use a fresh profile each run. Close any open Chrome using that profile before running.
CHROME_USER_DATA_DIR = None  # e.g. r'C:\Users\bigba\AppData\Local\Google\Chrome\User Data'
CHROME_PROFILE_DIR = None    # e.g. 'Default' or 'Profile 1'

# Define a custom print function
def print_to_csv(message: str):
    """Prints a message to the terminal and appends it to All_Outputs.csv."""
    print(message)  # Print to terminal
    with open(os.path.join(output_dir, 'All_Outputs.csv'), mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([message])  # Write the message as a new row

def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    return session

# The process_film function is no longer needed since we extract data directly from the list page

def setup_webdriver():
    """
    Create Chrome driver using undetected-chromedriver to avoid Cloudflare/captcha detection,
    mirroring the Genre 250s Chrome setup.
    """
    options = uc.ChromeOptions()
    # Prefer normal window (undetected_chromedriver is already less detectable; headless can still be flagged)
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Optional: use existing Chrome profile for Letterboxd login
    if CHROME_USER_DATA_DIR and os.path.isdir(CHROME_USER_DATA_DIR):
        options.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}")
        if CHROME_PROFILE_DIR:
            options.add_argument(f"--profile-directory={CHROME_PROFILE_DIR}")
    # Omit version_main so undetected-chromedriver matches your installed Chrome (avoids mismatch after updates).
    driver = uc.Chrome(options=options, use_subprocess=True)
    return driver


def debug_list_page(max_items=5):
    """Load one list page with Selenium and print what we can capture from the list alone."""
    base_url = 'https://letterboxd.com/asset/list/stand-up-comedy-a-comprehensive-list/by/rating/'
    url = f'{base_url}page/1/'
    print("=== DEBUG: List page only (no film pages) ===\n")
    driver = setup_webdriver()
    try:
        print(f"Loading: {url}")
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'li.posteritem'))
        )
        time.sleep(1)
        html = driver.page_source
    finally:
        driver.quit()

    soup = BeautifulSoup(html, 'html.parser')
    ul_poster = soup.find('ul', class_='poster-list')
    print(f"  ul.poster-list found: {ul_poster is not None}")
    if not ul_poster:
        print(f"  Total <ul> on page: {len(soup.find_all('ul'))}")
        for i, u in enumerate(soup.find_all('ul', limit=5)):
            kids = u.find_all('li', class_='posteritem')
            print(f"    ul[{i}] class={u.get('class')!r}  li.posteritem count={len(kids)}")

    film_grid = ul_poster or soup.find('ul')
    if film_grid:
        items = film_grid.find_all('li', class_='posteritem')
        print(f"\n  li.posteritem count: {len(items)}")
        for i, li in enumerate(items[:max_items]):
            print(f"\n  --- Item {i + 1} ---")
            for k, v in sorted(li.attrs.items()):
                val = (v[:80] + '...') if isinstance(v, str) and len(v) > 80 else v
                print(f"    {k}: {val!r}")
            react = li.find('div', class_='react-component')
            if react and react.attrs:
                r = {k: v for k, v in react.attrs.items() if k.startswith('data-')}
                if r:
                    print(f"    div.react-component data-*: {r}")
            a = li.find('a', href=lambda h: h and '/film/' in h)
            if a:
                print(f"    link: {a.get('href')!r}  title: {a.get('title')!r}")
    print("\n=== End debug ===\n")


def process_page(session, url, max_films, min_watches, approved_films, page_html=None):
    try:
        # Check if we've already hit the max_films limit before processing the page
        if len(approved_films) >= max_films:
            return False, []

        if page_html is None:
            response = session.get(url, timeout=10)
            page_html = response.content
        soup = BeautifulSoup(page_html, 'html.parser')

        film_grid = soup.find('ul', class_='poster-list')
        if not film_grid:
            film_grid = soup.find('ul')
            if film_grid and not film_grid.find_all('li', class_='posteritem'):
                film_grid = None

        if not film_grid:
            print_to_csv("Warning: Could not find poster list container")
            return False, []

        film_elements = film_grid.find_all('li', class_='posteritem')
        if not film_elements:
            print_to_csv("Warning: Could not find posteritem elements")
            return False, []
        
        print_to_csv(f"Found {len(film_elements)} film elements on this page")
        
        # Debug: Show attributes of first film element
        if film_elements:
            first_film = film_elements[0]
            # print_to_csv(f"First film attributes: {dict(first_film.attrs)}")
        
        film_data_list = []
        
        for i, film in enumerate(film_elements, 1):
            # Check if we've hit the max_films limit before processing each film
            if len(approved_films) >= max_films:
                print_to_csv(f"\nReached maximum number of films ({max_films}). Stopping...")
                return False, film_data_list
                
            try:
                # Data lives on div.react-component (list page), not on the li
                react_component = film.find('div', class_='react-component')
                film_url = (react_component.get('data-item-link') if react_component else None) or film.get('data-item-link')
                film_title = (react_component.get('data-item-full-display-name') if react_component else None) or film.get('data-item-full-display-name')
                film_id = (react_component.get('data-film-id') if react_component else None) or film.get('data-film-id')

                if film_url and film_title and film_id:
                    # Extract year from the full display name
                    year = ''
                    if '(' in film_title and ')' in film_title:
                        year = film_title[film_title.rindex('(')+1:film_title.rindex(')')]
                        title = film_title[:film_title.rindex('(')].strip()
                    else:
                        title = film_title
                    
                    # Check for duplicate using title+year combination
                    film_key = f"{title}_{year}"
                    if film_key in approved_films:
                        print_to_csv(f"❌ {film_title} - Not added (Duplicate film)")
                        continue

                    film_data = {
                        'title': title,
                        'year': year,
                        'id': film_id,
                        'original_order': len(approved_films) + 1
                    }

                    if LIST_ONLY:
                        # Use list data only; no per-film page visit
                        approved_films.add(film_key)
                        film_data_list.append(film_data)
                        print_to_csv(f"✅ {film_title}")
                        continue

                    # Visit film page to check rating count
                    try:
                        full_film_url = f"https://letterboxd.com{film_url}"
                        film_response = session.get(full_film_url, timeout=10)
                        film_soup = BeautifulSoup(film_response.content, 'html.parser')
                        json_ld = film_soup.find('script', type='application/ld+json')
                        rating_count = 0
                        if json_ld:
                            try:
                                json_text = json_ld.string.strip()
                                if '/* <![CDATA[ */' in json_text:
                                    json_text = json_text.replace('/* <![CDATA[ */', '').replace('/* ]]> */', '')
                                film_data_json = json.loads(json_text)
                                rating_count = film_data_json.get('aggregateRating', {}).get('ratingCount', 0)
                            except (json.JSONDecodeError, KeyError):
                                rating_count = 0
                        if rating_count < min_watches:
                            print_to_csv(f"❌ {film_title} - Not added (Rating count: {rating_count} < {min_watches})")
                            continue
                        approved_films.add(film_key)
                        film_data_list.append(film_data)
                        print_to_csv(f"✅ {film_title}")
                    except Exception as e:
                        print_to_csv(f"❌ {film_title} - Error checking rating count: {str(e)}")
                        continue
                else:
                    print_to_csv(f"Warning: Missing data for film element {i}")
                    # Debug: Show what attributes this element actually has
                    available_attrs = dict(film.attrs)
                    print_to_csv(f"Available attributes for element {i}: {available_attrs}")
                    
            except Exception as e:
                print_to_csv(f"Error processing film element {i}: {str(e)}")
                continue
                    
        has_next = bool(soup.find('a', class_='next'))
        return has_next, film_data_list
        
    except Exception as e:
        print_to_csv(f"Error processing page: {str(e)}")
        return False, []

def main():
    base_url = 'https://letterboxd.com/asset/list/stand-up-comedy-a-comprehensive-list/by/rating/'
    min_watches = 1000
    max_films = 100

    if DEBUG_LIST_PAGE:
        debug_list_page(max_items=5)
        return

    session = create_session()
    all_movies = []
    approved_films = set()
    page = 1
    driver = setup_webdriver()
    try:
        while True:
            url = f'{base_url}page/{page}/'
            print_to_csv(f"\n=== Page {page} ===")
            print_to_csv(f"Progress: {len(all_movies)}/{max_films} movies collected")
            try:
                try:
                    driver.get(url)
                except (NoSuchWindowException, WebDriverException) as e:
                    # Mirror Update Letterboxd Lists behavior: log clearly and abort if the browser dies early.
                    print_to_csv("❌ Browser window closed while loading list page; aborting Comedy 100 updates.")
                    raise e

                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'li.posteritem'))
                )
                time.sleep(random.uniform(0.8, 1.5))
                page_html = driver.page_source
            except Exception as e:
                print_to_csv(f"Warning: Could not load list page: {e}")
                break
            has_next, page_data = process_page(
                session, url, max_films, min_watches, approved_films, page_html=page_html
            )
            all_movies.extend(page_data)
            if len(approved_films) >= max_films or not has_next:
                break
            page += 1
            time.sleep(1)
    finally:
        driver.quit()

    # Save to CSV maintaining original order
    list_name = "stand_up_comedy"  # You can modify this based on your list
    filepath = os.path.join(output_dir, f"{list_name}.csv")
    
    # Write to CSV
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Year', 'LetterboxdID'])  # Header row
        for movie in all_movies:  # Will naturally maintain the order from processing
            writer.writerow([movie['title'], movie['year'], movie['id']])
    
    print_to_csv(f"Scraped {len(all_movies)} movies")

if __name__ == "__main__":
    main()