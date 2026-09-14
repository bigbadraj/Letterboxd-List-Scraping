import sys
import time
from selenium import webdriver
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchWindowException, NoSuchElementException, TimeoutException
import pandas as pd
import os
import platform
import glob
import subprocess
import pyautogui
from tqdm import tqdm
import csv
from datetime import datetime
import logging
import traceback
from credentials_loader import load_credentials

# Explicit driver.quit() handles shutdown; suppress duplicate destructor cleanup.
try:
    uc.Chrome.__del__ = lambda self: None
except Exception:
    pass

# Configure logging to only show the message after - INFO -
logging.basicConfig(level=logging.INFO, format='%(message)s')

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
base_dir = paths['base_dir']

# Profile reuse is opt-in. A clean profile avoids Chrome profile locks and
# starts reliably; set LETTERBOXD_CHROME_USER_DATA_DIR to reuse a profile.
CHROME_USER_DATA_DIR = os.environ.get('LETTERBOXD_CHROME_USER_DATA_DIR')
CHROME_PROFILE_DIR = os.environ.get('LETTERBOXD_CHROME_PROFILE_DIR', 'Default')

# Define a custom print function
def log_and_print(message: str):
    """Prints a message to the terminal and appends it to All_Outputs.csv."""
    try:
        print(message)
    except UnicodeEncodeError:
        # Keep logging usable in legacy Windows consoles that do not support Unicode.
        encoding = getattr(sys.stdout, 'encoding', None) or 'ascii'
        print(message.encode(encoding, errors='replace').decode(encoding))
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    with open(os.path.join(output_dir, 'All_Outputs.csv'), mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([message])  # Write the message as a new row

def find_import_button(driver):
    """Return the visible, enabled Letterboxd import button for the current page state."""
    selectors = [
        "button.js-import-trigger",
        "button[aria-label*='Import titles using our CSV format']",
        ".list-import-link",
    ]

    for selector in selectors:
        for element in driver.find_elements(By.CSS_SELECTOR, selector):
            if element.is_displayed() and element.is_enabled():
                return element

    for element in driver.find_elements(By.XPATH, "//button[contains(translate(@aria-label, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'import') or normalize-space(.)='Import']"):
        if element.is_displayed() and element.is_enabled():
            return element

    raise NoSuchElementException("Could not locate Letterboxd import button using current selectors.")


def find_save_button(driver):
    """Return the visible, enabled Letterboxd save button for the current page state."""
    selectors = [
        "button[type='submit'] .label",
        "button.button-neue.-primary",
        "button[type='submit']",
        "#list-edit-save",
    ]

    for selector in selectors:
        if selector.startswith("#"):
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                if element.is_displayed() and element.is_enabled():
                    return element
            except Exception:
                continue
            continue

        for element in driver.find_elements(By.CSS_SELECTOR, selector):
            if element.is_displayed() and element.is_enabled():
                return element

    for element in driver.find_elements(By.XPATH, "//button[normalize-space(.)='Save' or @type='submit']"):
        if element.is_displayed() and element.is_enabled():
            return element

    raise NoSuchElementException("Could not locate Letterboxd save button using current selectors.")


def hard_reload_page(driver, log_and_print_func=None, reason="page failed to load"):
    """Force a full browser reload to recover from blank or half-rendered Letterboxd pages."""
    try:
        current_url = driver.current_url or "(unknown url)"
        if log_and_print_func:
            log_and_print_func(f"⚠️ Hard reloading page after {reason}: {current_url}")
        driver.execute_script("window.location.reload(true);")
        time.sleep(3)
        return True
    except Exception as error:
        if log_and_print_func:
            log_and_print_func(f"⚠️ Hard reload attempt failed: {error}")
        try:
            driver.refresh()
            time.sleep(3)
            return True
        except Exception:
            return False


def safe_click_import_button(driver, log_and_print_func):
    """
    Safely click the import button with proper waiting and retry logic.
    This prevents the 'saving' element from obscuring the button.
    """
    for attempt in range(1, 3):
        try:
            saving_elements = driver.find_elements(By.CSS_SELECTOR, ".saving")
            if saving_elements:
                log_and_print_func("✅ Saving indicator detected, waiting briefly...")
                time.sleep(3)

            start_time = time.time()
            while time.time() - start_time < 5:
                try:
                    import_button = find_import_button(driver)
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", import_button)
                    import_button.click()
                    log_and_print_func("✅ Successfully clicked import button.")
                    time.sleep(2)
                    return driver
                except Exception:
                    time.sleep(0.5)

            log_and_print_func(
                f"⚠️ Import button was not clickable within 5 seconds (attempt {attempt}/2)."
            )
        except Exception as error:
            log_and_print_func(f"⚠️ Attempt {attempt} failed to click import button: {error}")

    raise RuntimeError("Import button was not clickable after two 5-second attempts.")


def wait_for_import_results(driver, timeout=90):
    """Wait for Letterboxd to finish importing instead of sleeping a fixed duration."""
    WebDriverWait(driver, timeout, poll_frequency=0.5).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, ".add-import-films-to-list"))
    )


def wait_for_list_editor(driver, timeout=30):
    """Wait until the list editor is ready for another action."""
    WebDriverWait(driver, timeout, poll_frequency=0.5).until(
        lambda current_driver: _has_import_button(current_driver)
    )


def _has_import_button(driver):
    try:
        button = find_import_button(driver)
        return button.is_displayed() and button.is_enabled()
    except Exception:
        return False


def wait_for_visible_input(driver, name, timeout=15):
    """Return the visible, enabled input when Letterboxd renders duplicate form fields."""
    def find_input(current_driver):
        for element in current_driver.find_elements(By.NAME, name):
            if element.is_displayed() and element.is_enabled():
                return element
        return False

    return WebDriverWait(driver, timeout, poll_frequency=0.5).until(find_input)


def wait_for_sign_in_form(driver, timeout=30):
    """Wait for the visible Letterboxd sign-in fields and control to render."""
    def find_form(current_driver):
        try:
            username_input = next(
                (element for element in current_driver.find_elements(By.NAME, "username")
                 if element.is_displayed() and element.is_enabled()),
                None,
            )
            password_input = next(
                (element for element in current_driver.find_elements(By.NAME, "password")
                 if element.is_displayed() and element.is_enabled()),
                None,
            )
            sign_in_button = next(
                (element for element in current_driver.find_elements(
                    By.XPATH,
                    "//button[@type='submit' or contains(translate(normalize-space(.), "
                    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sign in') "
                    "or contains(translate(@aria-label, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                    "'abcdefghijklmnopqrstuvwxyz'), 'sign in')] | //input[@type='submit']",
                )
                 if element.is_displayed()),
                None,
            )
            if username_input and password_input and sign_in_button:
                return username_input, password_input, sign_in_button
        except (NoSuchWindowException, NoSuchElementException):
            return False
        return False

    return WebDriverWait(driver, timeout, poll_frequency=0.5).until(find_form)


def is_security_check_page(driver):
    """Return whether Letterboxd or its edge security layer is asking for verification."""
    try:
        title = (driver.title or '').lower()
        body_text = str(
            driver.execute_script("return document.body ? document.body.innerText : ''") or ''
        ).lower()
        security_markers = (
            'just a moment',
            'checking your browser',
            'verify you are human',
            'verifying you are human',
            'security check',
            'performing security verification',
            'enable javascript and cookies to continue',
        )
        return any(marker in f"{title} {body_text}" for marker in security_markers)
    except Exception:
        return False


def wait_for_visible_sign_in_link(driver, timeout=15):
    """Return the visible sign-in link after the homepage finishes rendering."""
    def find_link(current_driver):
        for element in current_driver.find_elements(By.CSS_SELECTOR, ".sign-in-menu a"):
            if element.is_displayed() and element.is_enabled():
                return element
        return False

    return WebDriverWait(driver, timeout, poll_frequency=0.5).until(find_link)


def import_simple_list(driver, edit_url, csv_path, csv_file_name, log_and_print_func):
    """Replace a list from a CSV without changing its description."""
    driver.get(edit_url)
    time.sleep(2)
    driver = safe_click_import_button(driver, log_and_print_func)

    log_and_print_func(f"✅ Selecting CSV file: {csv_file_name}")
    time.sleep(1)
    pyautogui.hotkey('alt', 'd')
    time.sleep(1)
    pyautogui.typewrite(os.path.dirname(csv_path), interval=0.1)
    pyautogui.press('enter')
    time.sleep(1)
    pyautogui.hotkey('alt', 'n')
    time.sleep(0.5)
    pyautogui.typewrite(csv_file_name, interval=0.1)
    time.sleep(1)
    pyautogui.press('enter')

    wait_for_import_results(driver)
    try:
        hide_successful_matches_handle = driver.find_element(
            By.CSS_SELECTOR, ".import-toggle .handle"
        )
        hide_successful_matches_handle.click()
        log_and_print_func("✅ Clicked the 'Hide Successful Matches' handle.")
    except Exception as error:
        log_and_print_func(f"⚠️ Failed to click the successful-matches handle: {error}")

    time.sleep(5)
    try:
        replace_substitute = driver.find_element(
            By.CSS_SELECTOR, "label[for='replace-original'] .substitute"
        )
        replace_substitute.click()
        log_and_print_func("✅ Selected replacement of the existing list.")
    except Exception as error:
        log_and_print_func(f"⚠️ Failed to select list replacement: {error}")

    time.sleep(1)
    log_and_print_func("✅ Clicking the 'Add films to list' button.")
    driver.find_element(By.CSS_SELECTOR, ".add-import-films-to-list").click()
    time.sleep(5)
    save_button = find_save_button(driver)
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", save_button)
    try:
        save_button.click()
    except Exception:
        driver.execute_script("arguments[0].click();", save_button)
    time.sleep(7)
    return driver


def is_blank_or_unusable_page(driver):
    """Detect a blank, error, or otherwise unusable browser page before continuing."""
    try:
        current_url = (driver.current_url or '').lower()
        if 'about:blank' in current_url or 'chrome-error://' in current_url or 'chrome://crash' in current_url:
            return True
    except Exception:
        return True

    try:
        title = (driver.title or '').lower()
        body_text = str(driver.execute_script("return document.body ? document.body.innerText : ''") or '').lower()
        combined = f"{title} {body_text}"

        error_markers = [
            'error on page',
            'this page is not available',
            'this page isn\'t available',
            'page not available',
            'page could not be loaded',
            'something went wrong',
            'unable to load',
            'internal server error',
            'bad gateway',
            'service unavailable',
            'page error',
            'not available',
        ]

        if any(marker in combined for marker in error_markers):
            return True

        if len(body_text.strip()) < 20:
            return True
    except Exception:
        return True

    try:
        username_present = bool(driver.find_elements(By.NAME, "username")) or bool(driver.find_elements(By.ID, "username"))
        password_present = bool(driver.find_elements(By.NAME, "password")) or bool(driver.find_elements(By.ID, "password"))
        return not (username_present and password_present)
    except Exception:
        return False


def create_chrome_driver(log_and_print_func):
    """Create a fresh undetected Chrome driver for Letterboxd."""
    options = uc.ChromeOptions()
    options.page_load_strategy = 'eager'
    options.add_argument("--window-size=1280,900")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option(
        "prefs",
        {
            "profile.default_content_setting_values.javascript": 1,
        },
    )
    chrome_profile_available = bool(CHROME_USER_DATA_DIR and os.path.isdir(CHROME_USER_DATA_DIR))
    if chrome_profile_available:
        try:
            chrome_processes = subprocess.run(
                ["tasklist", "/FI", "IMAGENAME eq chrome.exe"],
                capture_output=True,
                text=True,
                check=False,
            ).stdout
            chrome_profile_available = "chrome.exe" not in chrome_processes.lower()
        except Exception:
            pass

    if chrome_profile_available:
        log_and_print_func(
            f"✅ Reusing Chrome profile: {CHROME_USER_DATA_DIR} ({CHROME_PROFILE_DIR})"
        )
        options.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}")
        if CHROME_PROFILE_DIR:
            options.add_argument(f"--profile-directory={CHROME_PROFILE_DIR}")
    elif CHROME_USER_DATA_DIR:
        log_and_print_func(
            "⚠️ Chrome is already running; using a clean browser profile to avoid a locked-profile session."
        )

    def _detect_chrome_major_version():
        try:
            import winreg
            for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                for subkey in (
                    r"Software\Google\Chrome\BLBeacon",
                    r"Software\WOW6432Node\Google\Chrome\BLBeacon",
                ):
                    try:
                        k = winreg.OpenKey(hive, subkey)
                        v, _ = winreg.QueryValueEx(k, "version")
                        if v:
                            return int(str(v).split(".", 1)[0])
                    except Exception:
                        continue
        except Exception:
            pass
        try:
            import subprocess
            out = subprocess.check_output(["chrome", "--version"], stderr=subprocess.STDOUT, text=True)
            for token in out.split():
                if token and token[0].isdigit() and "." in token:
                    return int(token.split(".", 1)[0])
        except Exception:
            pass
        return None

    log_and_print_func("✅ Starting undetected Chrome driver.")
    chrome_major = _detect_chrome_major_version()
    if chrome_major:
        driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_major)
    else:
        driver = uc.Chrome(options=options, use_subprocess=True)
    driver.set_page_load_timeout(60)
    log_and_print_func("✅ Chrome driver started.")
    return driver


def login_to_letterboxd(driver, username, password, log_and_print_func):
    """Sign in with retries and browser reset if a blank/invalid page is encountered."""
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            log_and_print_func(f"✅ Attempting Letterboxd sign-in (attempt {attempt}/{max_attempts}).")
            driver.get("https://letterboxd.com/sign-in/")
            log_and_print_func("✅ Loading Letterboxd sign-in page directly.")
            time.sleep(3)
            if is_security_check_page(driver):
                log_and_print_func(
                    "⚠️ Letterboxd security verification is active. Complete it in Chrome; "
                    "waiting up to 2 minutes for the sign-in form."
                )
                username_input, password_input, sign_in_button = wait_for_sign_in_form(
                    driver, timeout=120
                )
            else:
                username_input, password_input, sign_in_button = wait_for_sign_in_form(
                    driver, timeout=20
                )
            username_input.clear()
            username_input.send_keys(username)
            password_input.clear()
            password_input.send_keys(password)
            WebDriverWait(driver, 90, poll_frequency=0.5).until(
                lambda current_driver: sign_in_button.is_displayed() and sign_in_button.is_enabled()
            )
            sign_in_button.click()

            try:
                WebDriverWait(driver, 30).until(lambda current_driver: "/sign-in" not in current_driver.current_url)
            except TimeoutException:
                if is_blank_or_unusable_page(driver):
                    log_and_print_func("⚠️ Blank or unusable sign-in page detected; forcing a hard reload.")
                    hard_reload_page(driver, log_and_print_func, "sign-in page failure")
                    if is_blank_or_unusable_page(driver):
                        raise RuntimeError("Blank page after sign-in attempt.")
                log_and_print_func(
                    "⚠️ Sign In did not redirect. Complete any Letterboxd security "
                    "verification in Chrome; waiting up to 2 minutes."
                )
                WebDriverWait(driver, 120, poll_frequency=0.5).until(
                    lambda current_driver: "/sign-in" not in current_driver.current_url
                )

            if "/sign-in" not in driver.current_url:
                log_and_print_func("✅ Successfully signed in to Letterboxd.")
                return driver

        except Exception as e:
            error_text = str(e)
            session_lost = any(
                marker in error_text.lower()
                for marker in (
                    "invalid session id",
                    "not connected to devtools",
                    "browser has closed the connection",
                    "target window already closed",
                )
            )
            try:
                page_url = driver.current_url
                page_title = driver.title
                body_text = str(driver.execute_script("return document.body ? document.body.innerText : ''") or '')
                no_js_marker = bool(driver.execute_script(
                    "return document.documentElement && "
                    "document.documentElement.classList.contains('no-js');"
                ))
                log_and_print_func(
                    f"⚠️ Sign-in page state: url={page_url!r}, title={page_title!r}, "
                    f"body_length={len(body_text)}, no_js_marker={no_js_marker}, "
                    f"body_preview={body_text[:200]!r}"
                )
            except Exception as page_state_error:
                if not session_lost:
                    log_and_print_func(f"⚠️ Could not inspect sign-in page state: {page_state_error}")
            log_and_print_func(f"⚠️ Sign-in attempt {attempt} failed: {error_text}")
            if attempt < max_attempts:
                retry_delay = min(60, 10 * attempt)
                log_and_print_func(
                    f"✅ Waiting {retry_delay} seconds before the next sign-in attempt."
                )
                time.sleep(retry_delay)
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = create_chrome_driver(log_and_print_func)
                log_and_print_func("✅ Browser reset after failed sign-in; retrying.")
                continue
            raise e

    raise RuntimeError("Unable to sign in to Letterboxd after multiple attempts.")


def update_letterboxd_lists():
    # Load credentials
    credentials = load_credentials()
    
    # User credentials and file paths
    username = credentials['LETTERBOXD_USERNAME']
    password = credentials['LETTERBOXD_PASSWORD']
    output_csv_path = os.path.join(output_dir, 'update_results.csv')
    base_folder_path = output_dir
    # Initialize tracking variables so they exist even if we fail early
    results = []
    list_name = "INITIAL_SETUP"

    # Dictionary of lists to update
    lists_to_update_easy = {
        "top_250_action_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-action-narrative-feature/edit/",
        "top_250_adventure_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-adventure-narrative/edit/",
        "top_250_animation_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-animation-narrative/edit/",
        "top_250_comedy_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-comedy-narrative-feature/edit/",
        "top_250_crime_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-crime-narrative-feature/edit/",
        "top_250_drama_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-drama-narrative-feature/edit/",
        "top_250_family_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-family-narrative-feature/edit/",
        "top_250_fantasy_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-fantasy-narrative-feature/edit/",
        "top_250_history_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-history-narrative-feature/edit/",
        "top_250_horror_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-horror-narrative-feature/edit/",
        "top_250_music_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-music-narrative-feature/edit/",
        "top_250_mystery_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-mystery-narrative-feature/edit/",
        "top_250_romance_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-romance-narrative-feature/edit/",
        "top_250_science-fiction_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-science-fiction-narrative/edit/",
        "top_250_thriller_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-thriller-narrative/edit/",
        "top_250_western_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-western-narrative-feature/edit/",
        "top_250_war_rating": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-war-narrative-feature/edit/",
        "G_top_movies": "https://letterboxd.com/bigbadraj/list/top-100-g-rated-narrative-feature-films/edit/",
        "PG_top_movies": "https://letterboxd.com/bigbadraj/list/top-250-pg-rated-narrative-feature-films/edit/",
        "PG-13_top_movies": "https://letterboxd.com/bigbadraj/list/top-250-pg-13-rated-narrative-feature-films/edit/",
        "R_top_movies": "https://letterboxd.com/bigbadraj/list/top-250-r-rated-narrative-feature-films/edit/",
        "NC-17_top_movies": "https://letterboxd.com/bigbadraj/list/top-20-nc-17-rated-narrative-feature-films/edit/",
        "north_america_top_movies": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-north-american-narrative/edit/",
        "south_america_top_movies": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-south-american-narrative/edit/",
        "europe_top_movies": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-european-narrative/edit/",
        "asia_top_movies": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-asian-narrative-feature/edit/",
        "africa_top_movies": "https://letterboxd.com/bigbadraj/list/top-100-highest-rated-african-narrative-feature/edit/",
        "oceania_top_movies": "https://letterboxd.com/bigbadraj/list/top-75-highest-rated-australian-narrative-1/edit/",
        "90_Minutes_or_Less_top_movies": "https://letterboxd.com/bigbadraj/list/the-top-250-highest-rated-films-of-90-minutes/edit/",
        "2_Hours_or_Less_top_movies": "https://letterboxd.com/bigbadraj/list/the-top-250-highest-rated-films-of-120-minutes/edit/",
        "3_Hours_or_Greater_top_movies": "https://letterboxd.com/bigbadraj/list/the-top-150-highest-rated-films-of-180-minutes/edit/",
        "4_Hours_or_Greater_top_movies": "https://letterboxd.com/bigbadraj/list/the-top-20-highest-rated-films-of-240-minutes/edit/",
        "top_250_action_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-action-narrative-feature/edit/",
        "top_250_adventure_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-adventure-narrative/edit/",
        "top_250_animation_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-animation-narrative/edit/",
        "top_250_comedy_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-comedy-narrative-feature/edit/",
        "top_250_crime_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-crime-narrative-feature/edit/",
        "top_250_drama_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-drama-narrative-feature/edit/",
        "top_250_family_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-family-narrative-feature/edit/",
        "top_250_fantasy_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-fantasy-narrative-feature/edit/",
        "top_250_history_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-history-narrative-feature/edit/",
        "top_250_horror_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-horror-narrative-feature/edit/",
        "top_250_music_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-music-narrative-feature/edit/",
        "top_250_mystery_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-mystery-narrative-feature/edit/",
        "top_250_romance_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-romance-narrative-feature/edit/",
        "top_250_science-fiction_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-science-fiction-narrative/edit/",
        "top_250_thriller_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-thriller-narrative-feature/edit/",
        "top_250_western_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-western-narrative-feature/edit/",
        "top_250_war_popular": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-war-narrative-feature/edit/",
        "G_pop_movies": "https://letterboxd.com/bigbadraj/list/top-200-most-popular-g-rated-narrative-feature/edit/",
        "PG_pop_movies": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-pg-rated-narrative-feature/edit/",
        "PG-13_pop_movies": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-pg-13-rated-narrative/edit/",
        "R_pop_movies": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-r-rated-narrative-feature/edit/",
        "NC-17_pop_movies": "https://letterboxd.com/bigbadraj/list/top-25-most-popular-nc-17-rated-narrative/edit/",
        "north_america_pop_movies": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-north-american-narrative/edit/",
        "south_america_pop_movies": "https://letterboxd.com/bigbadraj/list/top-100-most-popular-south-american-narrative/edit/",
        "europe_pop_movies": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-european-narrative-feature/edit/",
        "asia_pop_movies": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-asian-narrative-feature/edit/",
        "africa_pop_movies": "https://letterboxd.com/bigbadraj/list/top-20-most-popular-african-narrative-feature/edit/",
        "oceania_pop_movies": "https://letterboxd.com/bigbadraj/list/top-150-most-popular-australian-narrative/edit/",
        "90_Minutes_or_Less_pop_movies": "https://letterboxd.com/bigbadraj/list/the-top-250-most-popular-films-of-90-minutes/edit/",
        "2_Hours_or_Less_pop_movies": "https://letterboxd.com/bigbadraj/list/the-top-250-most-popular-films-of-120-minutes/edit/",
        "3_Hours_or_Greater_pop_movies": "https://letterboxd.com/bigbadraj/list/the-top-75-most-popular-films-of-180-minutes/edit/",
        "4_Hours_or_Greater_pop_movies": "https://letterboxd.com/bigbadraj/list/the-top-5-most-popular-films-of-240-minutes/edit/",
    }

    personal_lists = {
        "Personal_sleepaway_camp_movies_ranked": "https://letterboxd.com/bigbadraj/list/sleepaway-camp-movies-ranked/edit/",
        "Personal_2026_releases_ranked": "https://letterboxd.com/bigbadraj/list/2026-releases-ranked/edit/",
        "Personal_2025_releases_ranked": "https://letterboxd.com/bigbadraj/list/2025-releases-ranked/edit/",
        "Personal_friday_the_13th_movies_ranked": "https://letterboxd.com/bigbadraj/list/friday-the-13th-movies-ranked/edit/",
        "Personal_halloween_movies_ranked": "https://letterboxd.com/bigbadraj/list/halloween-movies-ranked/edit/",
        "Personal_v_h_s_movies_ranked": "https://letterboxd.com/bigbadraj/list/v-h-s-movies-ranked/edit/",
        "Personal_mission_impossible_movies_ranked": "https://letterboxd.com/bigbadraj/list/mission-impossible-movies-ranked/edit/",
        "Personal_scream_movies_ranked": "https://letterboxd.com/bigbadraj/list/scream-movies-ranked/edit/",
        "Personal_2024_releases_ranked": "https://letterboxd.com/bigbadraj/list/2024-releases-ranked/edit/",
        "Personal_saw_movies_ranked": "https://letterboxd.com/bigbadraj/list/saw-movies-ranked/edit/",
        "Personal_nightmare_on_elm_street_movies_ranked": "https://letterboxd.com/bigbadraj/list/nightmare-on-elm-street-movies-ranked/edit/",
        "Personal_hannibal_movies_ranked": "https://letterboxd.com/bigbadraj/list/hannibal-movies-ranked/edit/",
        "Personal_marvel_movies_ranked": "https://letterboxd.com/bigbadraj/list/marvel-movies-ranked/edit/",
        "Personal_superhero_movies_ranked_1": "https://letterboxd.com/bigbadraj/list/superhero-movies-ranked-1/edit/",
        "Personal_dc_movies_ranked_1": "https://letterboxd.com/bigbadraj/list/dc-movies-ranked-1/edit/",
    }

    # Dictionary of lists to update with specific descriptions
    lists_with_descriptions = {
        "film_titles": {
            "url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-things-on-letterboxd/edit/",
            "description": "Minimum 1,000 ratings. Otherwise, anything on Letterboxd is eligible.\n\nLast Updated: {date}\n\n<a href=https://letterboxd.com/bigbadraj/list/the-official-list-index/> Check out more of the lists I update regularly! </a>"
        },
        "box_office_real": {
            "url": "https://letterboxd.com/bigbadraj/list/top-250-highest-grossing-movies-of-all-time-1/edit/",
            "description": "According to Box Office Mojo.\n\nLast Updated: {date}\n\n<a href=https://letterboxd.com/bigbadraj/list/the-official-list-index/> Check out more of the lists I update regularly! </a>"
        },
        "box_office_inflated": {
            "url": "https://letterboxd.com/bigbadraj/list/top-250-highest-grossing-domestic-movies/edit/",
            "description": "According to Box Office Mojo.\n\nLast Updated: {date}\n\n<a href=https://letterboxd.com/bigbadraj/list/the-official-list-index/> Check out more of the lists I update regularly! </a>"
        }
    }

    # Handle special lists
    special_lists = {
        "rating_filtered_movie_titles1": {
            "url": "https://letterboxd.com/bigbadraj/list/top-2500-highest-rated-narrative-feature/edit/",
            "csv_file_name_1": "rating_filtered_movie_titles1.csv",
            "csv_file_name_2": "rating_filtered_movie_titles2.csv",
            "csv_file_name_3": "rating_filtered_movie_titles3.csv"
        },
        "popular_filtered_movie_titles1": {
            "url": "https://letterboxd.com/bigbadraj/list/top-2500-most-popular-narrative-feature-films/edit/",
            "csv_file_name_1": "popular_filtered_movie_titles1.csv",
            "csv_file_name_2": "popular_filtered_movie_titles2.csv",
            "csv_file_name_3": "popular_filtered_movie_titles3.csv"
        }
    }

    driver = None

    try:
        driver = create_chrome_driver(log_and_print)
        log_and_print("✅ Navigating to Letterboxd homepage.")
        try:
            driver = login_to_letterboxd(driver, username, password, log_and_print)
        except NoSuchWindowException as e:
            log_and_print("❌ Browser window closed while checking sign-in; aborting updates.")
            raise e

        # Loop through each list to update
        for list_name, edit_url in lists_to_update_easy.items():
            log_and_print(f"✅ Updating list: {list_name}")
            
            # Initialize a flag to track errors
            has_error = False

            try:
                # Check required files before opening the list in Chrome.
                csv_file_name = f"{list_name}.csv"
                csv_file_path = os.path.join(output_dir, csv_file_name)
                matching_files = glob.glob(os.path.join(base_folder_path, f"stats_{list_name}*.txt"))

                if not os.path.exists(csv_file_path):
                    log_and_print(f"❌ CSV file not found: {csv_file_name}")
                    log_and_print(f"⏩ Skipping list update for {list_name} - required file does not exist")
                    results.append({
                        'list_name': list_name,
                        'status': f'Failed to update: CSV file {csv_file_name} not found'
                    })
                    continue

                if not matching_files:
                    log_and_print(f"❌ Stats text file not found for {list_name}")
                    log_and_print(f"⏩ Skipping list update for {list_name} - required file does not exist")
                    results.append({
                        'list_name': list_name,
                        'status': f'Failed to update: stats text file for {list_name} not found'
                    })
                    continue

                with open(matching_files[0], 'r', encoding='utf-8') as txt_file:
                    file_contents = txt_file.read()

                # Open the list only after all required files are available.
                driver.get(edit_url)
                time.sleep(2)

                # Click the Import button
                driver = safe_click_import_button(driver, log_and_print)

                # Step 3: Select the correct CSV file
                log_and_print(f"✅ Selecting CSV file: {csv_file_name}")
                time.sleep(1)

                # Use Alt + D to focus on the address bar of the file dialog
                pyautogui.hotkey('alt', 'd')
                time.sleep(1)

                # Type the folder path to navigate to the Outputs folder
                pyautogui.typewrite(output_dir, interval=0.1)
                pyautogui.press('enter')  # Navigate to the folder
                time.sleep(1)

                # Use Alt + N to focus on the filename box at the bottom of the file dialog
                pyautogui.hotkey('alt', 'n')
                time.sleep(0.5)
                
                # Type the filename to filter/search for it (Windows file dialogs filter as you type)
                pyautogui.typewrite(csv_file_name, interval=0.1)
                time.sleep(1)  
                pyautogui.press('enter')  # Select the filtered file

                time.sleep(2)  

                wait_for_import_results(driver)

                # Step 4: Click the "Hide Successful Matches" button
                try:
                    hide_successful_matches_handle = driver.find_element(By.CSS_SELECTOR, ".import-toggle .handle")
                    hide_successful_matches_handle.click()
                    log_and_print("✅ Clicked the 'Hide Successful Matches' handle.")
                except Exception as e:
                    log_and_print(f"❌ Failed to click the handle: {str(e)}")

                time.sleep(5)  

                # Step 5: Click the "Replace existing list with imported films" checkbox
                try:
                    replace_substitute = driver.find_element(By.CSS_SELECTOR, "label[for='replace-original'] .substitute")
                    replace_substitute.click()
                    log_and_print("✅ Clicked the 'Replace existing list with imported films' substitute icon.")
                except Exception as e:
                    log_and_print(f"❌ Failed to click the substitute icon: {str(e)}")

                time.sleep(1)  

                # Step 6: Click the "Add films to list" button
                log_and_print("✅ Clicking the 'Add films to list' button.")
                add_films_button = driver.find_element(By.CSS_SELECTOR, ".add-import-films-to-list")
                add_films_button.click()
                time.sleep(5)  

                # Step 7: Replace the existing list description with the copied text file contents
                if 'file_contents' in locals():
                    description_field = driver.find_element(By.CSS_SELECTOR, "textarea[name='notes']")  

                    try:
                        description_field.clear()  
                        description_field.send_keys(file_contents)  
                        log_and_print("✅ Successfully added text using send_keys.")
                    except Exception as e:
                        log_and_print(f"❌ Failed to add text using send_keys: {str(e)}")

                # Step 8: Save the changes
                time.sleep(1)
                log_and_print("✅ Saving the changes.")
                save_button = find_save_button(driver)
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", save_button)
                try:
                    save_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", save_button)
                time.sleep(7)

                # Log success or failure based on the error flag
                if has_error:
                    results.append({
                        'list_name': list_name,
                        'status': 'Failed to update: Missing text file'
                    })
                else:
                    results.append({
                        'list_name': list_name,
                        'status': 'Successfully updated'
                    })
                log_and_print(f"✅ Successfully updated list: {list_name}")

            except Exception as e:
                log_and_print(f"❌ Failed to update list: {list_name}. Error: {str(e)}")
                results.append({
                    'list_name': list_name,
                    'status': f'Failed to update: {str(e)}'
                })
                continue  

        # Handle corrected personal lists when their CSVs exist.
        for list_name, edit_url in personal_lists.items():
            csv_file_name = f"{list_name}.csv"
            csv_file_path = os.path.join(output_dir, csv_file_name)
            if not os.path.exists(csv_file_path):
                log_and_print(f"⏩ Skipping {list_name}: {csv_file_name} does not exist.")
                results.append({
                    'list_name': list_name,
                    'status': f'Skipped: CSV file {csv_file_name} not found'
                })
                continue

            log_and_print(f"✅ Updating personal list: {list_name}")
            try:
                import_simple_list(
                    driver,
                    edit_url,
                    csv_file_path,
                    csv_file_name,
                    log_and_print,
                )
                results.append({
                    'list_name': list_name,
                    'status': 'Successfully updated'
                })
                log_and_print(f"✅ Successfully updated personal list: {list_name}")
            except Exception as error:
                log_and_print(f"❌ Failed to update personal list: {list_name}. Error: {error}")
                results.append({
                    'list_name': list_name,
                    'status': f'Failed to update: {error}'
                })

        # Handle lists with specific descriptions
        for list_name, details in lists_with_descriptions.items():
            log_and_print(f"✅ Updating list: {list_name}")

            # Initialize a flag to track errors
            has_error = False

            try:
                # Check the CSV before opening the list in Chrome.
                csv_file_name = f"{list_name}.csv"
                csv_file_path = os.path.join(output_dir, csv_file_name)

                if not os.path.exists(csv_file_path):
                    log_and_print(f"❌ CSV file not found: {csv_file_name}")
                    log_and_print(f"⏩ Skipping list update for {list_name} - file does not exist")
                    results.append({
                        'list_name': list_name,
                        'status': f'Failed to update: CSV file {csv_file_name} not found'
                    })
                    continue

                # Open the list only after the required file is available.
                driver.get(details["url"])
                time.sleep(2)

                # Click the Import button
                driver = safe_click_import_button(driver, log_and_print)

                # Step 3: Select the correct CSV file

                log_and_print(f"✅ Selecting CSV file: {csv_file_name}")
                time.sleep(1) 

                # Use Alt + D to focus on the address bar of the file dialog
                pyautogui.hotkey('alt', 'd')
                time.sleep(1) 

                # Type the folder path to navigate to the Outputs folder
                pyautogui.typewrite(output_dir, interval=0.1)
                pyautogui.press('enter')  # Navigate to the folder
                time.sleep(1)

                # Use Alt + N to focus on the filename box at the bottom of the file dialog
                pyautogui.hotkey('alt', 'n')
                time.sleep(0.5)
                
                # Type the filename to filter/search for it (Windows file dialogs filter as you type)
                pyautogui.typewrite(csv_file_name, interval=0.1)
                time.sleep(1)  
                pyautogui.press('enter')  # Select the filtered file
                wait_for_import_results(driver)

                # Step 4: Click the "Hide Successful Matches" button
                try:
                    hide_successful_matches_handle = driver.find_element(By.CSS_SELECTOR, ".import-toggle .handle")
                    hide_successful_matches_handle.click()
                    log_and_print("✅ Clicked the 'Hide Successful Matches' handle.")
                except Exception as e:
                    log_and_print(f"❌ Failed to click the handle: {str(e)}")

                time.sleep(7)

                # Step 5: Click the "Replace existing list with imported films" checkbox
                try:
                    replace_substitute = driver.find_element(By.CSS_SELECTOR, "label[for='replace-original'] .substitute")
                    replace_substitute.click()
                    log_and_print("✅ Clicked the 'Replace existing list with imported films' substitute icon.")
                except Exception as e:
                    log_and_print(f"❌ Failed to click the substitute icon: {str(e)}")
                    
                time.sleep(1) 

                # Step 6: Click the "Add films to list" button
                log_and_print("✅ Clicking the 'Add films to list' button.")
                add_films_button = driver.find_element(By.CSS_SELECTOR, ".add-import-films-to-list")
                add_films_button.click()
                time.sleep(5)  

                # Step 7: Replace the existing list description with the new description
                current_date = time.strftime("%m/%d/%Y")  
                description = details["description"].format(date=current_date)  

                description_field = driver.find_element(By.CSS_SELECTOR, "textarea[name='notes']")  

                try:
                    description_field.clear()  
                    description_field.send_keys(description) 
                    log_and_print("✅ Successfully added text using send_keys.")
                except Exception as e:
                    log_and_print(f"❌ Failed to add text using send_keys: {str(e)}")

                # Step 8: Save the changes
                time.sleep(1)
                log_and_print("✅ Saving the changes.")
                save_button = find_save_button(driver)
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", save_button)
                try:
                    save_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", save_button)
                time.sleep(7)

                # Log success or failure based on the error flag
                if has_error:
                    results.append({
                        'list_name': list_name,
                        'status': 'Failed to update: Missing text file'
                    })
                else:
                    results.append({
                        'list_name': list_name,
                        'status': 'Successfully updated'
                    })
                log_and_print(f"✅ Successfully updated list: {list_name}")

            except Exception as e:
                log_and_print(f"❌ Failed to update list: {list_name}. Error: {str(e)}")
                results.append({
                    'list_name': list_name,
                    'status': f'Failed to update: {str(e)}'
                })
                continue  

        # Handle special lists
        for list_name, details in special_lists.items():
            log_and_print(f"✅ Updating special list: {list_name}")

            try:
                # Check every required CSV and the stats text before opening the list.
                csv_file_names = [
                    details["csv_file_name_1"],
                    details["csv_file_name_2"],
                    details["csv_file_name_3"],
                ]
                missing_csv_files = [
                    file_name for file_name in csv_file_names
                    if not os.path.exists(os.path.join(output_dir, file_name))
                ]
                matching_files = glob.glob(os.path.join(base_folder_path, f"{list_name[:15]}*.txt"))

                if missing_csv_files:
                    log_and_print(f"❌ CSV file(s) not found: {', '.join(missing_csv_files)}")
                    log_and_print(f"⏩ Skipping special list update for {list_name} - required file does not exist")
                    results.append({
                        'list_name': list_name,
                        'status': f'Failed to update: CSV file(s) {", ".join(missing_csv_files)} not found'
                    })
                    continue

                if not matching_files:
                    log_and_print(f"❌ Stats text file not found for {list_name}")
                    log_and_print(f"⏩ Skipping special list update for {list_name} - required file does not exist")
                    results.append({
                        'list_name': list_name,
                        'status': f'Failed to update: stats text file for {list_name} not found'
                    })
                    continue

                with open(matching_files[0], 'r', encoding='utf-8') as txt_file:
                    file_contents = txt_file.read()

                # Open the list only after all required files are available.
                csv_file_name = details["csv_file_name_1"]
                driver.get(details["url"])
                time.sleep(2)

                # Click the Import button
                driver = safe_click_import_button(driver, log_and_print)

                # Step 3: Import the first CSV file
                log_and_print("✅ Importing the first CSV file.")

                log_and_print(f"✅ Selecting CSV file: {csv_file_name}")
                time.sleep(1)  

                # Use Alt + D to focus on the address bar of the file dialog
                pyautogui.hotkey('alt', 'd')
                time.sleep(1) 

                # Type the folder path to navigate to the Outputs folder
                pyautogui.typewrite(output_dir, interval=0.1)
                pyautogui.press('enter')  # Navigate to the folder
                time.sleep(1)

                # Use Alt + N to focus on the filename box at the bottom of the file dialog
                pyautogui.hotkey('alt', 'n')
                time.sleep(0.5)
                
                # Type the filename to filter/search for it (Windows file dialogs filter as you type)
                pyautogui.typewrite(csv_file_name, interval=0.1)
                time.sleep(1)  
                pyautogui.press('enter')  # Select the filtered file

                wait_for_import_results(driver)

                # Step 3: Click the "Hide Successful Matches" button
                try:
                    hide_successful_matches_handle = driver.find_element(By.CSS_SELECTOR, ".import-toggle .handle")
                    hide_successful_matches_handle.click()
                    log_and_print("✅ Clicked the 'Hide Successful Matches' handle.")
                except Exception as e:
                    log_and_print(f"❌ Failed to click the handle: {str(e)}")

                time.sleep(7)  
                
                # Step 4: Click the "Replace existing list with imported films" checkbox
                try:
                    replace_substitute = driver.find_element(By.CSS_SELECTOR, "label[for='replace-original'] .substitute")
                    replace_substitute.click()
                    log_and_print("✅ Clicked the 'Replace existing list with imported films' substitute icon.")
                except Exception as e:
                    log_and_print(f"❌ Failed to click the substitute icon: {str(e)}")
                
                time.sleep(1)  

                # Step 5: Click the "Add films to list" button
                log_and_print("✅ Clicking the 'Add films to list' button.")
                add_films_button = driver.find_element(By.CSS_SELECTOR, ".add-import-films-to-list")
                add_films_button.click()
                time.sleep(5)  

                # Step 6: Replace the existing list description with the copied text file contents
                if 'file_contents' in locals():
                    description_field = driver.find_element(By.CSS_SELECTOR, "textarea[name='notes']") 

                    try:
                        description_field.clear()  
                        description_field.send_keys(file_contents) 
                        log_and_print("✅ Successfully added text using send_keys.")
                    except Exception as e:
                        log_and_print(f"❌ Failed to add text using send_keys: {str(e)}")

                # Step 7: Save the changes for the first import
                log_and_print("✅ Saving the changes for the first import.")
                save_button = find_save_button(driver)
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", save_button)
                try:
                    save_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", save_button)
                wait_for_list_editor(driver)

                # Step 8: Check if the second CSV file exists before attempting to upload
                log_and_print("✅ Checking second CSV file.")
                csv_file_name = details["csv_file_name_2"]
                csv_file_path = os.path.join(output_dir, csv_file_name)

                if not os.path.exists(csv_file_path):
                    log_and_print(f"❌ CSV file not found: {csv_file_name}")
                    log_and_print(f"⏩ Skipping second CSV import for {list_name} - file does not exist")
                    results.append({
                        'list_name': list_name,
                        'status': f'Failed to update: CSV file {csv_file_name} not found'
                    })
                    continue

                # Step 9: Click the Import button again
                log_and_print("✅ Clicking the Import button for the second time.")
                driver = safe_click_import_button(driver, log_and_print)

                # Step 10: Import the second CSV file
                log_and_print("✅ Importing the second CSV file.")

                log_and_print(f"✅ Selecting CSV file: {csv_file_name}")
                time.sleep(1)  

                # Use Alt + D to focus on the address bar of the file dialog
                pyautogui.hotkey('alt', 'd')
                time.sleep(1) 

                # Type the folder path to navigate to the Outputs folder
                pyautogui.typewrite(output_dir, interval=0.1)
                pyautogui.press('enter')  # Navigate to the folder
                time.sleep(1)

                # Use Alt + N to focus on the filename box at the bottom of the file dialog
                pyautogui.hotkey('alt', 'n')
                time.sleep(0.5)
                
                # Type the filename to filter/search for it (Windows file dialogs filter as you type)
                pyautogui.typewrite(csv_file_name, interval=0.1)
                time.sleep(1)  
                pyautogui.press('enter')  # Select the filtered file

                wait_for_import_results(driver)

                # Step 11: Click the "Hide Successful Matches" button again
                try:
                    hide_successful_matches_handle = driver.find_element(By.CSS_SELECTOR, ".import-toggle .handle")
                    hide_successful_matches_handle.click()
                    log_and_print("✅ Clicked the 'Hide Successful Matches' handle.")
                except Exception as e:
                    log_and_print(f"❌ Failed to click the handle: {str(e)}")

                time.sleep(7)

                # Step 11: Click the "Add films to list" button again
                log_and_print("✅ Clicking the 'Add films to list' button.")
                add_films_button = driver.find_element(By.CSS_SELECTOR, ".add-import-films-to-list")
                add_films_button.click()
                time.sleep(5)   

                # Step 12: Save the changes for the second import
                time.sleep(1)
                log_and_print("✅ Saving the changes for the second import.")
                save_button = find_save_button(driver)
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", save_button)
                try:
                    save_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", save_button)
                wait_for_list_editor(driver)

                # Step 13: Check if the third CSV file exists before attempting to upload
                log_and_print("✅ Checking third CSV file.")
                csv_file_name = details["csv_file_name_3"]
                csv_file_path = os.path.join(output_dir, csv_file_name)

                if not os.path.exists(csv_file_path):
                    log_and_print(f"❌ CSV file not found: {csv_file_name}")
                    log_and_print(f"⏩ Skipping third CSV import for {list_name} - file does not exist")
                    results.append({
                        'list_name': list_name,
                        'status': f'Failed to update: CSV file {csv_file_name} not found'
                    })
                    continue

                # Step 14: Click the Import button for the third time
                log_and_print("✅ Clicking the Import button for the third time.")
                driver = safe_click_import_button(driver, log_and_print)

                # Step 15: Import the third CSV file
                log_and_print("✅ Importing the third CSV file.")

                log_and_print(f"✅ Selecting CSV file: {csv_file_name}")
                time.sleep(1)  

                # Use Alt + D to focus on the address bar of the file dialog
                pyautogui.hotkey('alt', 'd')
                time.sleep(1) 

                # Type the folder path to navigate to the Outputs folder
                pyautogui.typewrite(output_dir, interval=0.1)
                pyautogui.press('enter')  # Navigate to the folder
                time.sleep(1)

                # Use Alt + N to focus on the filename box at the bottom of the file dialog
                pyautogui.hotkey('alt', 'n')
                time.sleep(0.5)
                
                # Type the filename to filter/search for it (Windows file dialogs filter as you type)
                pyautogui.typewrite(csv_file_name, interval=0.1)
                time.sleep(1)  
                pyautogui.press('enter')  # Select the filtered file

                wait_for_import_results(driver)

                # Step 16: Click the "Hide Successful Matches" button again
                try:
                    hide_successful_matches_handle = driver.find_element(By.CSS_SELECTOR, ".import-toggle .handle")
                    hide_successful_matches_handle.click()
                    log_and_print("✅ Clicked the 'Hide Successful Matches' handle.")
                except Exception as e:
                    log_and_print(f"❌ Failed to click the handle: {str(e)}")

                time.sleep(7)

                # Step 17: Click the "Add films to list" button again
                log_and_print("✅ Clicking the 'Add films to list' button.")
                add_films_button = driver.find_element(By.CSS_SELECTOR, ".add-import-films-to-list")
                add_films_button.click()
                time.sleep(5)   

                # Step 18: Save the changes for the third import
                time.sleep(1)
                log_and_print("✅ Saving the changes for the third import.")
                save_button = find_save_button(driver)
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", save_button)
                try:
                    save_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", save_button)
                wait_for_list_editor(driver)

                log_and_print(f"✅ Successfully updated special list: {list_name}")
                # Append success result for special list
                results.append({
                    'list_name': list_name,
                    'status': 'Successfully updated'
                })

            except Exception as e:
                log_and_print(f"❌ Failed to update special list: {list_name}. Error: {str(e)}")
                # Append failure result for special list
                results.append({
                    'list_name': list_name,
                    'status': f'Failed to update: {str(e)}'
                })
                continue  

    except Exception as e:
        log_and_print(f"❌ Failed to update list: {list_name}. Error: {str(e)}")
        log_and_print(traceback.format_exc())  
        results.append({
            'list_name': list_name,
            'status': f'Failed to update: {str(e)}'
        })

    finally:
        # Output the results to a CSV file
        log_and_print("✅ Outputting results to CSV file.")
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, index=False, mode='a', header=not os.path.exists(output_csv_path)) 

        # Close the browser
        time.sleep(5)
        log_and_print("✅ Closing the browser.")
        if driver is not None:
            driver.quit()

# Example usage
update_letterboxd_lists()