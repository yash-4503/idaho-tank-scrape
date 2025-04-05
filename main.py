import time
import logging
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

### STEP 1: Load CSV Files

def load_csv_data():
    """
    Load the three provided CSV files and return dataframes.
    Adjust the file names and parsing options as needed.
    """
    try:
        df_listing = pd.read_csv("listing-data.csv")
        df_lust = pd.read_csv("lust-events-data.csv")
        df_tanks = pd.read_csv("tanks-data.csv")
        logging.info("Successfully loaded CSV files.")
        return df_listing, df_lust, df_tanks
    except Exception as e:
        logging.error(f"Error loading CSV files: {e}")
        return None, None, None

### STEP 2: Scrape the Website

def setup_driver():
    """
    Set up Selenium Chrome driver in headless mode.
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
    Adjust selectors and actions based on the actual page.
    """
    url = "https://www2.deq.idaho.gov/waste/ustlust/Search.aspx"
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    
    try:
        # If there is a search button to click after optionally leaving criteria blank:
        search_button = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_btnSearch")))
        search_button.click()
        logging.info("Search triggered on the website.")
    except Exception as e:
        logging.error(f"Error triggering search: {e}")
    
    # Wait for the results table to load; adjust the selector to match the table element.
    try:
        wait.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_gridResults")))
    except Exception as e:
        logging.error(f"Error waiting for results table: {e}")

def scrape_results(driver):
    """
    Scrape tank results from the search results table.
    Adjust CSS selectors based on the website structure.
    """
    scraped_list = []
    try:
        # Find the table; adjust selector if needed.
        table = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_gridResults")
        rows = table.find_elements(By.TAG_NAME, "tr")
        
        logging.info(f"Found {len(rows)-1} result rows (excluding header).")
        
        # Skip header row; assume first row is header
        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            # Adjust indices based on actual table structure. For example:
            tank_id = cells[0].text.strip()
            facility_name = cells[1].text.strip()
            address = cells[2].text.strip()
            
            scraped_list.append({
                "Tank_ID": tank_id,
                "Facility_Name": facility_name,
                "Address": address
            })
    except Exception as e:
        logging.error(f"Error scraping table rows: {e}")
    
    return scraped_list

### STEP 3: Merge Scraped Data with CSV Data

def merge_data(scraped, df_listing, df_lust, df_tanks):
    """
    Merge scraped results with CSV data.
    The merge key is assumed to be "Tank_ID". Adjust merge logic as required.
    """
    df_scraped = pd.DataFrame(scraped)
    
    # Example merges – using left joins based on Tank_ID
    merged = pd.merge(df_scraped, df_listing, on="Tank_ID", how="left")
    merged = pd.merge(merged, df_lust, on="Tank_ID", how="left")
    merged = pd.merge(merged, df_tanks, on="Tank_ID", how="left")
    
    return merged

### MAIN FUNCTION

def main():
    # Load CSV files
    df_listing, df_lust, df_tanks = load_csv_data()
    if df_listing is None or df_lust is None or df_tanks is None:
        logging.error("CSV files not loaded properly. Exiting.")
        return

    # Set up Selenium driver and perform search
    driver = setup_driver()
    perform_search(driver)
    
    # Allow additional wait time if necessary for full table load
    time.sleep(3)
    
    # Scrape search results
    scraped_results = scrape_results(driver)
    driver.quit()
    
    if not scraped_results:
        logging.error("No scraped results found. Exiting.")
        return

    logging.info(f"Scraped {len(scraped_results)} tank records.")
    
    # Merge scraped data with CSV data
    final_df = merge_data(scraped_results, df_listing, df_lust, df_tanks)
    
    # Save final merged data
    output_file = "idaho_tank_scrape_output.csv"
    final_df.to_csv(output_file, index=False)
    logging.info(f"Scraping and merging completed. Output saved to {output_file}")

if __name__ == "__main__":
    main()
