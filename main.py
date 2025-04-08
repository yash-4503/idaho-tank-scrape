import time
import logging
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_csv_data():
    """
    Load the three provided CSV files.
    The files should be placed in the repo's root:
      - listing-data.csv
      - tanks-data.csv
      - lust-events-data.csv
    """
    try:
        df_listing = pd.read_csv("listing-data.csv", dtype=str)
        df_tanks = pd.read_csv("tanks-data.csv", dtype=str)
        df_lust = pd.read_csv("lust-events-data.csv", dtype=str)
        logging.info("CSV files loaded successfully.")
        return df_listing, df_tanks, df_lust
    except Exception as e:
        logging.error("Error loading CSV files: {}".format(e))
        return None, None, None

def setup_driver():
    """
    Set up the Selenium Chrome driver in headless mode.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver

def perform_search(driver):
    """
    Navigate to the Idaho DEQ UST/LUST search page and trigger the search.
    """
    url = "https://www2.deq.idaho.gov/waste/ustlust/Search.aspx"
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    
    try:
        # Click the search button to display all results.
        search_button = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_btnSearch")))
        search_button.click()
        logging.info("Search triggered on the website.")
        time.sleep(3)  # Allow time for the results table to load.
    except Exception as e:
        logging.error("Error triggering search: {}".format(e))

def scrape_results(driver):
    """
    Scrape tank results from the results table, and handle pagination.
    Extract additional columns: Facility Name, Status, Cleanup Method, and Address.
    """
    wait = WebDriverWait(driver, 10)
    all_data = []
    
    while True:
        try:
            # Wait for the results table to be present.
            table = wait.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_gridResults")))
            rows = table.find_elements(By.TAG_NAME, "tr")
            logging.info("Found {} rows on the current page (excluding header).".format(len(rows) - 1))
            
            # Process rows (skip header row, assume first row is header).
            for row in rows[1:]:
                cells = row.find_elements(By.TAG_NAME, "td")
                # Make sure there are enough cells; adjust indices if the structure changes.
                if len(cells) >= 5:
                    tank_id = cells[0].text.strip()
                    facility_name = cells[1].text.strip()
                    status = cells[2].text.strip()
                    cleanup_method = cells[3].text.strip()
                    address = cells[4].text.strip()
                    
                    all_data.append({
                        "Tank_ID": tank_id,
                        "Facility_Name": facility_name,
                        "Status": status,
                        "Cleanup_Method": cleanup_method,
                        "Address": address
                    })
            # Check for a "Next" button for pagination.
            try:
                next_button = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_gridResults_next")
                next_class = next_button.get_attribute("class")
                if "disabled" in next_class:
                    logging.info("No further pages to process.")
                    break
                else:
                    next_button.click()
                    logging.info("Navigating to the next page...")
                    time.sleep(3)  # Allow time for the next page to load.
            except Exception as e:
                logging.info("No next button found; assuming this is the last page.")
                break
        except Exception as e:
            logging.error("Error scraping results: {}".format(e))
            break

    return all_data

def merge_data(scraped, df_listing, df_tanks, df_lust):
    """
    Merge the scraped data with CSV data from the listing, tanks, and lust events files.
    Uses 'Tank_ID' as the merge key. Also, remove the 'Deleted' column if present.
    """
    df_scraped = pd.DataFrame(scraped)
    
    # Merge the scraped data with the CSV datasets.
    merged = pd.merge(df_scraped, df_listing, on="Tank_ID", how="left")
    merged = pd.merge(merged, df_tanks, on="Tank_ID", how="left")
    merged = pd.merge(merged, df_lust, on="Tank_ID", how="left")
    
    # Drop the "Deleted" column if it exists.
    if 'Deleted' in merged.columns:
        merged.drop(columns=['Deleted'], inplace=True)
    
    return merged

def main():
    # Load CSV data.
    df_listing, df_tanks, df_lust = load_csv_data()
    if df_listing is None or df_tanks is None or df_lust is None:
        logging.error("CSV data not loaded correctly. Exiting.")
        return

    # Set up Selenium driver and perform search.
    driver = setup_driver()
    perform_search(driver)
    
    # Scrape data across all pages.
    scraped_data = scrape_results(driver)
    driver.quit()
    
    if not scraped_data:
        logging.error("No data was scraped. Exiting.")
        return

    logging.info("Scraped a total of {} records.".format(len(scraped_data)))
    
    # Merge scraped data with CSV files.
    final_df = merge_data(scraped_data, df_listing, df_tanks, df_lust)
    
    # Save the final merged dataset; additional columns (Facility_Name, Status, Cleanup_Method) are now included.
    output_file = "idaho_tank_scrape_output.csv"
    final_df.to_csv(output_file, index=False)
    logging.info("Final merged data has been saved to '{}'.".format(output_file))

if __name__ == "__main__":
    main()
