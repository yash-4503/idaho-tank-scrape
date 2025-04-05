# idaho-tank-scrape

# Idaho Tank Scraper 🛢️

This project automates the extraction of tank information from a website using Tank_IDs. Each Tank_ID maps to a unique page. The script uses Selenium (headless Chrome) to load each page and scrape the tank name and location.

---

## ✅ Features

- Dynamic scraping using Selenium
- Headless Chrome for automation
- Structured CSV output
- Logs and error handling

---

## 🛠 Requirements

- Python 3.7+
- Google Chrome installed

---

## 📦 Setup

1. Clone this repo:
   ```bash
   git clone https://github.com/<your-username>/idaho-tank-scrape.git
   cd idaho-tank-scrape

2. Install Dependencies
   pip install -r requirements.txt

3. Running the Scraper
   python main.py

The final merged output will be saved as idaho_tank_scrape_output.csv.
