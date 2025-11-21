import re
import json
import csv
import io
from time import sleep

import requests
from bs4 import BeautifulSoup
import PyPDF2
from google import genai
from googleapiclient.discovery import build

import logger
from config import GEMINI_API_KEY, MODEL_NAME, GOOGLE_API_KEY, CUSTOM_SEARCH_ENGINE_ID, SEARCH_RESULTS_TO_CHECK, \
    TARGET_YEARS
from scraper import get_selenium_driver, scrape_company_website_for_report_urls

client = genai.Client(api_key=GEMINI_API_KEY)


# --- Helper Functions ---
def get_year_multiplier(year):
    if not year or not isinstance(year, int):
        return 0.0
    if year >= 2023: return 1.0
    if year == 2022: return 0.8
    if year == 2021: return 0.6
    if year == 2020: return 0.4
    if year == 2019: return 0.2
    return 0.0


def extract_json_from_response(response_text):
    if not response_text:
        return None

    response_text = response_text.strip()
    response_text = re.sub(r'^(```)?(json)?\s*', '', response_text, flags=re.IGNORECASE)
    response_text = re.sub(r'\s*(```)?$', '', response_text, flags=re.IGNORECASE)

    try:
        json_output = json.loads(response_text)
        return json_output
    except json.JSONDecodeError:
        try:
            match = re.search(r'(\[.*?]|\{.*?})', response_text, re.DOTALL)
            if match:
                return json.loads(match.group(1))
        except:
            pass

        return None


def fetch_content_snippet(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, timeout=5, headers=headers, allow_redirects=True)
        response.raise_for_status()
        content_type_header = response.headers.get('Content-Type', '').lower()
        snippet = ""
        title = ""

        if 'pdf' in content_type_header or url.lower().endswith('.pdf'):
            pdf_file = io.BytesIO(response.content)
            try:
                reader = PyPDF2.PdfReader(pdf_file)
                for i in range(min(3, len(reader.pages))):  # First 3 pages
                    page_text = reader.pages[i].extract_text()
                    if page_text:
                        snippet += page_text + "\n"
                meta = reader.metadata
                if meta and meta.title:
                    title = meta.title
                if not snippet: snippet = "PDF content extracted (text might be image-based or empty)."
            except Exception as e:
                snippet = f"Could not extract text from PDF: {e}"
                logger.error(f"PDF parsing error for {url}: {e}")
        elif 'html' in content_type_header:
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.title.string if soup.title else "No Title"
            text_elements = soup.find_all(['p', 'h1', 'h2', 'h3', 'article', 'div'])  # Basic text extraction
            raw_text = ' '.join(el.get_text(separator=' ', strip=True) for el in text_elements)
            snippet = ' '.join(raw_text.split()[:1000])
        else:
            snippet = "Content type not HTML or PDF."
            title = "Unknown Title"

        return title, snippet[:5000]  # Limit snippet size
    except requests.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return "Fetch Error", f"Could not fetch content from {url}."
    except Exception as e:
        logger.error(f"Generic error processing {url}: {e}")
        return "Processing Error", "Error during content processing."


def call_llm_for_analysis(url, title, snippet, number_of_retries=3, retry_delay=2):
    prompt = f"""
        Analyze the following web content:
        URL: {url}
        Page Title: {title}
        Content Snippet (first ~1000 words or ~3 pages of PDF):
        ---
        {snippet}
        ---
        Context: I am trying to find annual financial reports or web pages containing specific financial data for a company. The target financial years are primarily 2024, 2023, 2022, 2021, 2020, 2019.
    
        Instructions:
        1.  Determine the primary type of this content. Choose one:
            a.  'ANNUAL_FINANCIAL_REPORT_DOCUMENT': A direct link to a full annual financial report document (e.g., PDF, self-contained HTML report).
            b.  'FINANCIAL_DATA_PAGE': A webpage that presents summarized financial data (e.g., investor relations page, financial highlights, data table) but is not the full report document itself.
            c.  'NEWS_ARTICLE_OR_PRESS_RELEASE': Contains financial information but primarily as news or announcement.
            d.  'INVESTOR_HUB_OR_INDEX': A page linking to multiple reports or financial documents.
            e.  'OTHER': None of the above or irrelevant.
        2.  If type is 'ANNUAL_FINANCIAL_REPORT_DOCUMENT' or 'FINANCIAL_DATA_PAGE':
            a.  What is the primary financial year (REFYEAR) this content refers to? (e.g., if a report covers 'April 2023 - March 2024', REFYEAR is 2024). Extract the year as YYYY. If multiple years are present, pick the latest one clearly associated with a full set of annual data. If ambiguous or not found, state 'UNKNOWN'.
        3.  If type is 'FINANCIAL_DATA_PAGE':
            a.  For the identified REFYEAR, indicate if the following data points are likely present on the page:
                - Country of MNE Group headquarters: YES/NO/UNKNOWN
                - Number of employees worldwide: YES/NO/UNKNOWN
                - Net Turnover: YES/NO/UNKNOWN
                - Total assets: YES/NO/UNKNOWN
        4.  Is the URL a direct link to a downloadable file (e.g., ends in .pdf, or content suggests it's a self-contained document rather than an interactive webpage)? YES/NO/UNKNOWN
    
        Output your response STRICTLY in JSON format:
        {{
          "url": "{url}",
          "content_type": "...",
          "ref_year": "YYYY_OR_UNKNOWN",
          "is_direct_file_link": "YES/NO/UNKNOWN",
          "data_points_present": {{
            "country_hq": "YES/NO/UNKNOWN",
            "employees": "YES/NO/UNKNOWN",
            "net_turnover": "YES/NO/UNKNOWN",
            "total_assets": "YES/NO/UNKNOWN"
          }}
        }}
    """
    print(f"\n--- Sending to LLM for URL: {url} ---")
    print(f"Prompt (partial): {prompt[:500]}...")  # For debugging

    for attempt in range(number_of_retries):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=['User: ', prompt]
            )

            response_text = response.text
            print(f"LLM response: {response_text}")

            return extract_json_from_response(response_text)
        except Exception as e:
            logger.warning(f"Gemini call failed: {e}")
            if attempt < number_of_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
            else:
                logger.error(f"Max attempts reached. Returning...")
    return {}


def process_company_urls(all_urls_for_company):
    analyzed_docs_raw = []
    for url in all_urls_for_company:
        logger.info(f"Processing URL for LLM analysis: {url}")
        title, snippet = fetch_content_snippet(url)
        if title == "Fetch Error" or title == "Processing Error":
            analyzed_docs_raw.append({"url": url, "error": snippet, "type": "ERROR"})
            continue

        llm_result = call_llm_for_analysis(url, title, snippet)

        if not llm_result:
            analyzed_docs_raw.append({"url": url, "error": "LLM analysis failed", "type": "ERROR"})
            continue

        doc_info = {"url": url, "llm_ref_year_str": "UNKNOWN"}
        doc_info.update(llm_result)

        try:
            doc_info["llm_ref_year_str"] = doc_info.get("ref_year", "UNKNOWN")
            if doc_info["llm_ref_year_str"] != "UNKNOWN":
                doc_info["ref_year_int"] = int(doc_info["llm_ref_year_str"])
            else:
                doc_info["ref_year_int"] = None
        except ValueError:
            doc_info["ref_year_int"] = None
            doc_info["llm_ref_year_str"] = "UNKNOWN"

        analyzed_docs_raw.append(doc_info)

    potential_fin_reps = []
    potential_other_sources = []

    for doc in analyzed_docs_raw:
        if doc.get("error"):
            doc["calculated_score"] = 0
            doc["selection_category"] = "ERROR"
            potential_other_sources.append(doc)
            continue

        content_type = doc.get("content_type")
        ref_year_int = doc.get("ref_year_int")
        is_direct_link = doc.get("is_direct_file_link") == "YES"

        doc["calculated_score"] = 0
        doc["selection_category"] = "UNKNOWN"

        if content_type == 'ANNUAL_FINANCIAL_REPORT_DOCUMENT' and is_direct_link and ref_year_int is not None:
            score = 1.0 * get_year_multiplier(ref_year_int)
            doc["calculated_score"] = score
            doc["selection_category"] = "POTENTIAL_FIN_REP"
            potential_fin_reps.append(doc)

        elif content_type == 'FINANCIAL_DATA_PAGE' and ref_year_int is not None:
            data_points = doc.get("data_points_present", {})
            num_present = sum(1 for val in data_points.values() if val == "YES")
            par_sc = 0.0
            if num_present >= 2:
                par_sc = num_present / 4.0

            year_mult = get_year_multiplier(ref_year_int)
            score = par_sc * year_mult
            doc["calculated_score"] = score
            doc["selection_category"] = "POTENTIAL_OTHER_FINANCIAL_DATA_PAGE"
            potential_other_sources.append(doc)

        elif content_type == 'ANNUAL_FINANCIAL_REPORT_DOCUMENT' and not is_direct_link and ref_year_int is not None:
            par_sc = 0.5
            year_mult = get_year_multiplier(ref_year_int)
            score = par_sc * year_mult
            doc["calculated_score"] = score
            doc["selection_category"] = "POTENTIAL_OTHER_REPORT_LANDING_PAGE"
            potential_other_sources.append(doc)

        else:
            year_mult = get_year_multiplier(ref_year_int) if ref_year_int else 0
            data_points = doc.get("data_points_present", {})
            num_present = sum(1 for val in data_points.values() if val == "YES")
            par_sc = 0.0
            if num_present >= 2:
                par_sc = num_present / 4.0

            doc["calculated_score"] = par_sc * year_mult
            doc["selection_category"] = "POTENTIAL_OTHER_GENERIC"
            potential_other_sources.append(doc)

    potential_fin_reps.sort(key=lambda x: (x.get("calculated_score", 0), x.get("ref_year_int", 0)), reverse=True)

    chosen_fin_rep = None
    if potential_fin_reps:
        if potential_fin_reps[0].get("calculated_score", 0) > 0:
            chosen_fin_rep = potential_fin_reps[0]
            chosen_fin_rep["final_type_for_csv"] = "FIN_REP"

    all_potential_other_sources = []

    for fin_rep_doc in potential_fin_reps:
        if chosen_fin_rep is None or fin_rep_doc["url"] != chosen_fin_rep["url"]:
            other_candidate_doc = fin_rep_doc.copy()
            par_sc_for_demoted_fin_rep = 0.75
            year_mult = get_year_multiplier(other_candidate_doc.get("ref_year_int"))
            other_candidate_doc["calculated_score"] = par_sc_for_demoted_fin_rep * year_mult
            other_candidate_doc["selection_category"] = "DEMOTED_FIN_REP_AS_OTHER"
            other_candidate_doc["final_type_for_csv"] = "OTHER"
            all_potential_other_sources.append(other_candidate_doc)

    for other_doc in potential_other_sources:
        if chosen_fin_rep and other_doc["url"] == chosen_fin_rep["url"]:
            continue
        other_doc_copy = other_doc.copy()
        other_doc_copy["final_type_for_csv"] = "OTHER"
        all_potential_other_sources.append(other_doc_copy)

    all_potential_other_sources.sort(key=lambda x: (x.get("calculated_score", 0), x.get("ref_year_int", 0) if x.get(
        "ref_year_int") is not None else -1), reverse=True)

    final_other_reports = []
    seen_urls_for_output = set()
    if chosen_fin_rep:
        seen_urls_for_output.add(chosen_fin_rep["url"])

    for other_doc in all_potential_other_sources:
        if len(final_other_reports) >= 5:
            break
        if other_doc["url"] not in seen_urls_for_output:
            if other_doc.get("calculated_score", 0) > 0 or other_doc.get("ref_year_int") is not None:
                final_other_reports.append(other_doc)
                seen_urls_for_output.add(other_doc["url"])

    if len(final_other_reports) < 5:
        remaining_raw_docs_to_consider = []
        for doc in analyzed_docs_raw:
            if doc.get("error"): continue
            if doc["url"] not in seen_urls_for_output:
                doc_copy = doc.copy()
                doc_copy["final_type_for_csv"] = "OTHER"
                remaining_raw_docs_to_consider.append(doc_copy)

        def sort_key_for_fillers(d):
            year = d.get("ref_year_int", -1) if d.get("ref_year_int") is not None else -1
            content_type_pref = 0
            if d.get("content_type") == 'ANNUAL_FINANCIAL_REPORT_DOCUMENT':
                content_type_pref = 3
            elif d.get("content_type") == 'FINANCIAL_DATA_PAGE':
                content_type_pref = 2
            elif d.get("content_type") == 'INVESTOR_HUB_OR_INDEX':
                content_type_pref = 1
            return (year, content_type_pref, d.get("calculated_score", 0))

        remaining_raw_docs_to_consider.sort(key=sort_key_for_fillers, reverse=True)

        for doc_to_fill in remaining_raw_docs_to_consider:
            if len(final_other_reports) >= 5:
                break
            if doc_to_fill["url"] not in seen_urls_for_output:
                final_other_reports.append(doc_to_fill)
                seen_urls_for_output.add(doc_to_fill["url"])

    return chosen_fin_rep, final_other_reports


def search_google_for_links(api_key, cse_id, company_name, num_results, prompt):
    """Searches Google using Custom Search API and returns a list of links."""
    logger.info(f"Searching Google for '{company_name}' with prompt: '{prompt}'")
    links = []
    try:
        service = build("customsearch", "v1", developerKey=api_key)
        api_response = service.cse().list(q=prompt, cx=cse_id, num=num_results).execute()

        if 'items' in api_response and api_response['items']:
            for item in api_response['items']:
                link = item.get('link')
                if link:  # Only add if a link exists
                    links.append(link)
            logger.info(f"Found {len(links)} links for '{company_name}' with prompt.")
        else:
            logger.warning(f"No Google search results found for '{company_name}' with the specified criteria: {prompt}")

        return links

    except Exception as e:
        logger.error(f"Error during Google Custom Search for '{prompt}': {e}")
        return []  # Return empty list on error to avoid None downstream


def main():
    input_csv_file = 'discovery.csv'
    output_csv_file = 'discovery_output.csv'

    # Read all company data first to group searches by company
    company_data = {}
    with open(input_csv_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        original_fieldnames = reader.fieldnames
        for row in reader:
            company_id = row['ID']
            if company_id not in company_data:
                company_data[company_id] = {'NAME': row['NAME'], 'original_rows': []}
            company_data[company_id]['original_rows'].append(row)

    selenium_driver = get_selenium_driver()

    if not selenium_driver:
        logger.error("Failed to init selenium driver for website scraping.")
        return

    # Prepare output CSV
    with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=original_fieldnames, delimiter=';')
        writer.writeheader()

        for company_id, data in company_data.items():
            company_name = data['NAME']
            logger.info(f"\n--- Processing company: {company_name} (ID: {company_id}) ---")

            all_discovered_urls_for_company = set()  # Use a set to store unique URLs

            search_queries = [
                f"\"{company_name}\" annual consolidated financial statements report results FY \"2024\" filetype:pdf",
                f"\"{company_name}\" annual consolidated financial statements report results FY \"2023\" filetype:pdf",
                f"\"{company_name}\" investor relations financial reports",
                f"\"{company_name}\" sustainability report OR ESG report OR Environmental report OR Corporate report OR Responsibility report filetype:pdf",
                f"\"{company_name}\" financial highlights OR key figures",
                # Add more generic searches if needed
                f"site:*.{company_name.lower().replace(' ', '').replace('.', '')}.com investor OR financial OR report OR results OR Download filetype:pdf",
                # site specific search
                f"\"{company_name}\" \"annual report\" OR \"financial results\" 2023 OR 2024"
            ]

            # Add specific homepage search - this might be a separate step in your actual scraper
            # For this integration, let's assume you have a way to get the homepage and add it.
            # homepage_url = f"https://www.{company_name.lower().replace(' ', '').replace('.', '')}.com/" # Simplified homepage guess
            # all_discovered_urls_for_company.add(homepage_url) # Add homepage if you have it

            # You might also have URLs from your direct homepage scraping here.
            # For now, we'll just use Google Search results.
            # Example:
            # pre_scraped_urls = ["https://example.com/path/to/report1.pdf", "https://example.com/investors/"]
            # for url in pre_scraped_urls:
            #    all_discovered_urls_for_company.add(url)

            for query_num, search_string in enumerate(search_queries):
                logger.info(f"Executing Google search query {query_num + 1}/{len(search_queries)} for {company_name}")
                discovered_links = search_google_for_links(
                    GOOGLE_API_KEY,
                    CUSTOM_SEARCH_ENGINE_ID,
                    company_name,
                    SEARCH_RESULTS_TO_CHECK,
                    search_string
                )
                for link in discovered_links:
                    all_discovered_urls_for_company.add(link)

            logger.info(f"Starting direct website scrape for {company_name}...")
            try:
                urls_from_website = scrape_company_website_for_report_urls(
                    selenium_driver,
                    company_name,
                    TARGET_YEARS
                )
                all_discovered_urls_for_company.update(urls_from_website)
                logger.info(f"Added {len(urls_from_website)} URLs from direct website scrape for {company_name}")
            except Exception as e_scrape:
                logger.error(f"Error during direct website scrape for {company_name}: {e_scrape}")

            if not all_discovered_urls_for_company:
                logger.warning(f"No URLs found for {company_name} after all Google searches. Writing empty rows.")
                # Write 6 empty rows for this company as per competition format
                for _ in range(6):
                    writer.writerow({
                        "ID": company_id,
                        "NAME": company_name,
                        "TYPE": "FIN_REP" if _ == 0 else "OTHER",
                        "SRC": "",
                        "REFYEAR": ""
                    })
                continue  # Move to the next company

            logger.info(
                f"Total unique URLs collected for {company_name} before LLM processing: {len(all_discovered_urls_for_company)}")

            # Now pass the collected URLs to your processing function
            # Convert set to list for process_company_urls
            best_fin_rep, top_other_sources = process_company_urls(list(all_discovered_urls_for_company))

            # --- Write results to CSV in the 1+5 format ---
            output_csv_rows = []

            # Row 1: FIN_REP
            if best_fin_rep:
                output_csv_rows.append({
                    "ID": company_id, "NAME": company_name, "TYPE": "FIN_REP",
                    "SRC": best_fin_rep["url"],
                    "REFYEAR": str(best_fin_rep.get("ref_year_int", ""))
                })
            else:
                output_csv_rows.append({
                    "ID": company_id, "NAME": company_name, "TYPE": "FIN_REP",
                    "SRC": "", "REFYEAR": ""
                })

            # Rows 2-6: OTHER
            for i in range(5):
                if i < len(top_other_sources):
                    other_src = top_other_sources[i]
                    year_str = str(other_src.get("ref_year_int", "")) if other_src.get(
                        "ref_year_int") is not None else other_src.get("llm_ref_year_str", "")
                    if year_str == "UNKNOWN": year_str = ""
                    output_csv_rows.append({
                        "ID": company_id, "NAME": company_name, "TYPE": "OTHER",
                        "SRC": other_src["url"],
                        "REFYEAR": year_str
                    })
                else:
                    output_csv_rows.append({
                        "ID": company_id, "NAME": company_name, "TYPE": "OTHER",
                        "SRC": "", "REFYEAR": ""
                    })

            for csv_row_to_write in output_csv_rows:
                writer.writerow(csv_row_to_write)

            logger.info(f"Finished processing and writing for {company_name}")

        logger.info(f"\nProcessing complete. Output written to {output_csv_file}")

    if selenium_driver:
        selenium_driver.quit()


if __name__ == "__main__":
    main()
