import json
import time
import re
from urllib.parse import urljoin, urlparse
import csv
from typing import Set, List, Optional, Dict

from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from config import GOOGLE_API_KEY, CUSTOM_SEARCH_ENGINE_ID, TARGET_YEARS, REQUEST_DELAY, \
    SELENIUM_LOAD_DELAY, SEARCH_RESULTS_TO_CHECK

import logger


def get_selenium_driver() -> Optional[webdriver.Chrome]:
    """Initializes and returns a Selenium WebDriver."""
    logger.info("Initializing Selenium WebDriver...")
    try:
        chrome_options = ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")  # Standard window size
        chrome_options.add_argument("--no-sandbox")  # Often needed in Docker/CI
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')

        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        driver.set_page_load_timeout(10)
        driver.implicitly_wait(5)
        logger.info("Selenium WebDriver initialized successfully.")
        return driver
    except WebDriverException as e:
        logger.error(f"Failed to initialize Selenium WebDriver: {e}")
        return None


def search_google_for_website(api_key: str, cse_id: str, company_name: str, num_results: int = 1) -> Optional[str]:
    """Searches Google for the company's official website.
       Prioritizes results with domain names matching the company name.
    """
    logger.info(f"Searching Google for '{company_name}' official website...")
    try:
        service = build("customsearch", "v1", developerKey=api_key)
        query = f"{company_name} official website investor"
        res = service.cse().list(q=query, cx=cse_id, num=num_results).execute()

        if 'items' not in res or not res['items']:
            logger.warning(f"No Google search results found for '{company_name}' website.")
            return None

        # Prepare company name for domain matching (simple version)
        company_name_simplified = ''.join(filter(str.isalnum, company_name.lower().split(' ')[0]))

        candidate_urls = []
        for item in res['items'][:SEARCH_RESULTS_TO_CHECK]:  # Check a few top results
            url = item.get('link')
            if not url:
                continue

            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower().replace('www.', '')

            # Score based on how well the domain matches
            score = 0
            if company_name_simplified in domain:
                score += 2  # Strong match
            if "investor" in domain or "ir" in domain:  # Bonus if it looks like an IR domain
                score += 1

            candidate_urls.append({'url': url, 'score': score, 'title': item.get('title', '')})

        if not candidate_urls:
            logger.warning(f"No valid links from Google search for '{company_name}'.")
            return None

        # Sort by score (desc)
        candidate_urls.sort(key=lambda x: x['score'], reverse=True)

        best_match_url = candidate_urls[0]['url']
        logger.info(
            f"Selected potential official website for '{company_name}': {best_match_url} (Title: '{candidate_urls[0]['title']}')")
        return best_match_url

    except Exception as e:
        logger.error(f"Error during Google Custom Search for website: {e}")
        return None


def click_cookie_banner(driver: webdriver.Chrome, timeout: int = 1):
    """Attempts to find and click common cookie consent buttons."""
    common_texts = [
        'accept all', 'allow all', 'agree', 'got it', 'ok', 'okay', 'continue',
        'accept cookies', 'understand', 'i accept', 'allow cookies',
        'einverstanden', 'akzeptieren', 'zulassen', 'alle akzeptieren'
    ]
    # Prioritize buttons, then links, then divs
    selectors = []
    for text in common_texts:
        # Buttons
        selectors.append(
            f"//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]")
        selectors.append(
            f"//button[.//span[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]]")
        # Links
        selectors.append(
            f"//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]")
        # Divs that might be clickable
        selectors.append(
            f"//div[contains(@role, 'button') and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]")

    # Also look for IDs/classes common in cookie banners
    common_ids_classes = ['cookie', 'consent', 'banner', 'privacy', 'policy', 'gdpr']
    for term in common_ids_classes:
        selectors.append(f"//button[contains(@id, '{term}') or contains(@class, '{term}')]")
        selectors.append(f"//a[contains(@id, '{term}') or contains(@class, '{term}')]")

    for i, selector in enumerate(selectors):
        try:
            # Use a shorter timeout for each attempt to iterate quickly
            element = WebDriverWait(driver, 1 if i < len(selectors) - 1 else timeout).until(
                EC.element_to_be_clickable((By.XPATH, selector))
            )
            if element.is_displayed():
                logger.info(f"Attempting to click cookie element with selector: {selector}")
                driver.execute_script("arguments[0].click();", element)
                logger.info("Cookie banner element clicked.")
                time.sleep(0.5)  # Brief pause after click
                return True
        except TimeoutException:
            logger.debug(f"Cookie element not found or not clickable with selector: {selector}")
        except Exception as e:
            logger.warning(f"Error clicking cookie element with selector {selector}: {e}")

    logger.info("No standard cookie consent button found or clickable.")
    return False


def get_page_soup_with_selenium(driver: webdriver.Chrome, url: str, delay: int = SELENIUM_LOAD_DELAY) -> Optional[
    BeautifulSoup]:
    """Navigates to a URL using Selenium, handles cookies, and returns BeautifulSoup soup."""
    logger.info(f"Fetching with Selenium: {url}")
    try:
        driver.get(url)
        # Initial wait for page to potentially load dynamic content / cookie banner
        time.sleep(delay / 2.0)  # Split delay

        clicked_cookie = click_cookie_banner(driver, timeout=3)
        if clicked_cookie:
            time.sleep(delay / 2.0)  # Wait for page to settle after cookie click

        # Additional wait for JS rendering (if any) after potential cookie click
        WebDriverWait(driver, delay).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        return BeautifulSoup(driver.page_source, 'lxml')

    except TimeoutException:
        logger.warning(f"Timeout loading page {url} with Selenium.")
        # Still try to return whatever source is available if timeout occurred after partial load
        return BeautifulSoup(driver.page_source, 'lxml') if driver.page_source else None
    except WebDriverException as e:
        logger.error(f"WebDriverException loading {url} with Selenium: {e}")
    except Exception as e:
        logger.error(f"Generic error loading {url} with Selenium: {e}")
    return None


def find_navigation_page_url(base_url: str, soup: BeautifulSoup, page_keywords: List[str], page_type_name: str) -> \
        Optional[str]:
    """
    Generalized function to find a navigation page (e.g., Investor Relations, Reports).
    Returns the URL of the best candidate page.
    """
    logger.info(f"Searching for '{page_type_name}' page link on {base_url}...")
    candidate_links = []

    for link_tag in soup.find_all('a', href=True):
        link_text = link_tag.get_text(strip=True).lower()
        link_href_attr = link_tag['href']  # Keep original for urljoin

        # Skip mailto, JavaScript, or fragment links, or empty hrefs
        if not link_href_attr or link_href_attr.startswith(('mailto:', 'javascript:', '#')):
            continue

        normalized_href = link_href_attr.lower()

        for i, keyword in enumerate(page_keywords):
            # Check if keyword is in link text or href
            # Prioritize text matches slightly
            text_match = keyword in link_text
            # Be more careful with href matches to avoid overly broad matches (e.g. 'sec' in 'section')
            href_match = f"/{keyword.replace(' ', '')}/" in normalized_href or \
                         f"/{keyword.replace(' ', '-')}/" in normalized_href or \
                         normalized_href.endswith(keyword.replace(' ', '')) or \
                         normalized_href.endswith(keyword.replace(' ', '-')) or \
                         f"{keyword.replace(' ', '')}." in normalized_href  # e.g. investors.example.com

            if text_match or href_match:
                potential_url = urljoin(base_url, link_href_attr)

                # Basic check to ensure it's likely an HTML page and on the same primary domain
                parsed_potential_url = urlparse(potential_url)
                parsed_base_url = urlparse(base_url)
                if parsed_potential_url.netloc.endswith(parsed_base_url.netloc.replace("www.", "")):  # Allow subdomains
                    if not any(potential_url.lower().endswith(ext) for ext in
                               ('.pdf', '.xls', '.xlsx', '.doc', '.docx', '.zip', '.jpg', '.png')):
                        candidate_links.append({
                            'url': potential_url,
                            'text': link_tag.get_text(strip=True),
                            'priority': i,  # Based on keyword list order
                            'text_match_bonus': 2 if text_match else 0  # Bonus for text match
                        })
                        break  # Found a keyword for this link_tag

    if not candidate_links:
        logger.warning(f"Could not find a clear '{page_type_name}' link on the current page using keywords.")
        return None

    # Sort by priority (lower is better), then by text_match_bonus (higher is better), then by length of text (shorter, more direct text is often better)
    candidate_links.sort(key=lambda x: (x['priority'], -x['text_match_bonus'], len(x['text'])))

    selected_url = candidate_links[0]['url']
    logger.info(f"Found potential '{page_type_name}' page: {selected_url} (Text: '{candidate_links[0]['text']}')")
    return selected_url


def extract_report_urls_from_page(driver: webdriver.Chrome, page_url: str, base_url: str, target_years: List[str]) -> \
        Set[str]:
    """
    Extracts plausible report URLs (especially PDFs) from a given page.
    Focuses on links containing report keywords and target years.
    """
    logger.info(f"Extracting report URLs from {page_url} for years: {target_years}...")
    found_urls: Set[str] = set()

    soup = get_page_soup_with_selenium(driver, page_url)
    if not soup:
        return found_urls

    report_keywords = [
        'annual report', 'financial report', 'form 10-k', '10-k', '20-f', 'sec filing',
        'financial results', 'financial statements', 'shareholder report', 'annual accounts',
        'consolidated financial', 'statutory accounts',
        'jahresbericht', 'geschäftsbericht', 'finanzbericht',  # German
        'rapport annuel', 'états financiers',  # French
        "Ετήσια Αναφορά", "Οικονομική Αναφορά", "Έντυπο 10-K", "10-K", "20-F",
        "Κατάθεση SEC", "Οικονομικά Αποτελέσματα", "Οικονομικές Καταστάσεις",
        "Αναφορά Μετόχων", "Ετήσιοι Λογαριασμοί", "Ενοποιημένες Οικονομικές",
        "Νομικοί Λογαριασμοί" # Greek
        # Add more languages / terms as needed
    ]
    # Keywords that are good indicators but might also appear on quarterly pages
    secondary_keywords = ['results', 'report', 'filing', 'financials', 'accounts',
                          "Αποτελέσματα", "Αναφορά", "Κατάθεση", "Οικονομικά", "Λογαριασμοί"]

    avoid_keywords = [
        'quarterly', 'q1', 'q2', 'q3', 'q4', 'interim', 'half-year', 'halbjahres', 'semi-annual',
        'quartalsbericht', 'presentation', 'earnings call', 'webcast', 'investor day', 'fact sheet', 'summary',
        "Τριμηνιαίο", "Ενδιάμεσο", "Εξαμηνιαίο", "Τριμηνιαία Έκθεση",   "Παρουσίαση",
        "Τηλεδιάσκεψη Αποτελεσμάτων", "Ημερίδα Επενδυτών", "Ενημερωτικό Δελτίο", "Δελτίο Γεγονότων"
        "Περίληψη"
    ]

    year_regexes = {year: re.compile(rf'\b{year}\b') for year in target_years}
    # Regex for any 4-digit year (e.g., 199X, 20XX)
    any_year_regex = re.compile(r'\b(19[89]\d|20\d{2})\b')

    for link_tag in soup.find_all('a', href=True):
        link_text = link_tag.get_text(strip=True).lower()
        original_href = link_tag['href']

        if not original_href or original_href.startswith(('mailto:', 'javascript:', '#')):
            continue

        full_url = urljoin(page_url, original_href)  # Use page_url as base for relative links from current page
        # Simple check to try and stay on the same broader domain
        if not urlparse(full_url).netloc.endswith(urlparse(base_url).netloc.replace("www.", "")):
            if not any(kw in full_url.lower() for kw in
                       ['sec.gov', 'sedar.com', 'filing', 'document']):  # Allow known filing sites
                logger.debug(f"Skipping off-domain link: {full_url} (base: {base_url})")
                continue

        normalized_href = original_href.lower()

        # --- Scoring Logic ---
        score = 0
        year_found = None

        # 1. Check for Target Years explicitly
        for target_year_str in target_years:
            if year_regexes[target_year_str].search(link_text) or year_regexes[target_year_str].search(normalized_href):
                score += 5  # Strong indicator
                year_found = target_year_str
                break

        # If no target year, check for any recent year
        if not year_found:
            match = any_year_regex.search(link_text) or any_year_regex.search(normalized_href)
            if match:
                year_found_general = match.group(0)
                if int(year_found_general) >= (int(target_years[0]) - 5):  # Within last 5 years from most recent target
                    score += 2
                    # year_found = year_found_general # Don't assign to year_found unless it's a target year for scoring

        # 2. Check for primary report keywords
        for rk in report_keywords:
            if rk in link_text or rk.replace(" ", "") in normalized_href:
                score += 3
                break  # One primary keyword is enough for this bonus

        # 3. Check for PDF
        if normalized_href.endswith('.pdf'):
            score += 4  # PDFs are highly preferred

        # 4. Check for secondary keywords (if no primary ones hit hard)
        if score < 5:  # If not a strong candidate yet
            for sk in secondary_keywords:
                if sk in link_text or sk in normalized_href:
                    score += 1
                    break

        # 5. Penalize for "avoid" keywords, unless strong positive signals exist
        is_avoided = False
        for ak in avoid_keywords:
            if ak in link_text or ak in normalized_href:
                is_avoided = True
                if score > 5:  # Strong candidate, maybe it's "Annual Report and Q4 Results"
                    score -= 1  # Slight penalty
                else:
                    score -= 3  # Heavier penalty for weaker candidates
                break

        # --- Decision ---
        # Require a decent score OR (PDF and a year)
        if score >= 4 or (normalized_href.endswith('.pdf') and year_found and score >= 2):
            logger.debug(
                f"Candidate report link: '{link_text}' ({full_url}), Score: {score}, Year: {year_found if year_found else 'N/A'}")
            found_urls.add(full_url)

    if not found_urls:
        logger.info(f"No report-like URLs extracted based on keywords/years from {page_url}. Trying broad PDF scan.")
        # Fallback: if the page URL itself looks like a directory listing or general downloads page
        if any(term in page_url.lower() for term in ['/reports', '/financials', '/downloads', '/archive', '/filings']):
            for link_tag in soup.find_all('a', href=True):
                original_href = link_tag['href']
                if original_href and original_href.lower().endswith('.pdf'):
                    full_url = urljoin(page_url, original_href)
                    if urlparse(full_url).netloc.endswith(urlparse(base_url).netloc.replace("www.", "")):
                        logger.debug(f"Fallback PDF added: {full_url}")
                        found_urls.add(full_url)

    logger.info(f"Found {len(found_urls)} potential report URLs on {page_url}.")
    return found_urls


def scrape_company_website_for_report_urls(driver: webdriver.Chrome, company_name: str, target_years: List[str]) -> Set[
    str]:
    """
    Main workflow to scrape a company's website for report URLs.
    1. Finds official website via Google.
    2. Navigates to homepage.
    3. Tries to find Investor Relations / Financials / Reports pages.
    4. Scans these pages for report links.
    Returns a set of unique URLs.
    """
    all_potential_report_urls: Set[str] = set()

    # 1. Find official website
    base_url = search_google_for_website(GOOGLE_API_KEY, CUSTOM_SEARCH_ENGINE_ID, company_name, num_results=3)
    if not base_url:
        logger.warning(f"Could not find a website for {company_name} via Google Search.")
        return all_potential_report_urls
    all_potential_report_urls.add(base_url)  # Add homepage itself as a candidate for LLM

    # 2. Get homepage soup
    time.sleep(REQUEST_DELAY)
    homepage_soup = get_page_soup_with_selenium(driver, base_url)
    if not homepage_soup:
        logger.warning(f"Could not fetch homepage content from {base_url}.")
        return all_potential_report_urls  # Return base_url if homepage fetch fails

    # 3. Find key navigation pages (Investor Relations, Financial Reports, etc.)
    # Define keywords for different types of pages you want to find
    ir_keywords = ['investor relations', 'investors', 'für investoren', 'investisseur', 'shareholder information',
                   "Επενδυτικές Σχέσεις", "Επενδυτές", "Για Επενδυτές",  "Επενδυτής",  "Πληροφορίες Μετόχων" # Greek
                   ]
    reports_keywords = [
        'financial reports', 'financial results', 'annual reports', 'sec filings', 'reports', 'publications',
        'financial statements', 'berichte', 'rapports financiers', 'downloads', 'archive',
        "Οικονομικές Αναφορές", "Οικονομικά Αποτελέσματα", "Ετήσιες Αναφορές",
        "Καταθέσεις", "Ρυθμιστικές Αρχές", "Καταθέσεις SEC", "Αναφορές", "Εκδόσεις",
        "Οικονομικές Καταστάσεις", "Λήψεις", "Αρχείο",  # Greek
    ]

    pages_to_scan: Set[str] = {base_url}  # Start with homepage

    # Try to find an Investor Relations page
    ir_page_url = find_navigation_page_url(base_url, homepage_soup, ir_keywords, "Investor Relations")
    if ir_page_url:
        pages_to_scan.add(ir_page_url)

    # Try to find a general "Reports" or "Financials" page (could be on homepage or IR page)
    # Scan from IR page if found, otherwise from homepage
    soup_to_search_reports_nav = homepage_soup
    base_for_reports_nav = base_url
    if ir_page_url:
        time.sleep(REQUEST_DELAY)
        ir_page_soup = get_page_soup_with_selenium(driver, ir_page_url)
        if ir_page_soup:
            soup_to_search_reports_nav = ir_page_soup
            base_for_reports_nav = ir_page_url

    reports_page_url = find_navigation_page_url(base_for_reports_nav, soup_to_search_reports_nav, reports_keywords,
                                                "Financial Reports")
    if reports_page_url:
        pages_to_scan.add(reports_page_url)

    logger.info(f"Pages to scan for reports for {company_name}: {pages_to_scan}")

    # 4. Extract report URLs from each identified key page
    for page_to_scan_url in pages_to_scan:
        time.sleep(REQUEST_DELAY)
        urls_from_page = extract_report_urls_from_page(driver, page_to_scan_url, base_url, target_years)
        all_potential_report_urls.update(urls_from_page)

    logger.info(
        f"Collected {len(all_potential_report_urls)} unique potential report URLs for {company_name} from its website.")
    return all_potential_report_urls


def main(company_name):
    driver = get_selenium_driver()
    if not driver:
        logger.error("Failed to initialize Selenium WebDriver. Exiting.")
        return

    try:
        logger.info(f"\n--- Starting website scan for {company_name} ---")
        pre_scraped_urls = scrape_company_website_for_report_urls(driver, company_name, TARGET_YEARS)

        logger.info(f"\n--- Pre-scraped URLs for {company_name} ---")
        if pre_scraped_urls:
            for url in pre_scraped_urls:
                logger.info(url)
        else:
            logger.info("No URLs found.")

        return list(pre_scraped_urls)

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main execution for {company_name}: {e}", exc_info=True)
    finally:
        if driver:
            driver.quit()
            logger.info("Selenium WebDriver quit.")
