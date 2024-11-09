from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

# Initialize the WebDriver
driver = webdriver.Chrome()

# Open the URL
url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_long_term_rate&field_tdr_date_value=2024"
driver.get(url)

# Wait for the dropdown to load
try:
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "select"))
    )
    print("Dropdown menu loaded successfully.")
except Exception as e:
    print(f"Error loading the page: {e}")
    driver.quit()
    exit()

# Locate the data_type dropdown and select the time period dropdown
data_type_dropdown = Select(driver.find_element(By.TAG_NAME, "select"))
time_period_dropdown = Select(driver.find_element(By.ID, "edit-field-tdr-date-value"))
time_period_dropdown.select_by_visible_text("- All -")

# Function to select an option and click "Apply"
def select_option_and_apply(driver, option):
    data_type_dropdown = Select(driver.find_element(By.TAG_NAME, "select"))
    name = data_type_dropdown.options[option].text
    data_type_dropdown.select_by_visible_text(name)
    print(f"Selected: {name}")
    
    # Click the "Apply" button
    apply_button = driver.find_element(By.ID, "edit-submit-dfu-tool-page")
    apply_button.click()

    # Wait for the table to reload
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
    )
    return name

# Function to extract table data from the current page
def extract_table_data():
    table = driver.find_element(By.TAG_NAME, "table")
    rows = table.find_elements(By.TAG_NAME, "tr")
    
    # Extract headers
    headers = [header.text for header in rows[0].find_elements(By.TAG_NAME, "th")]
    print(f"Headers: {headers}")

    # Extract rows of data
    data = []
    for row in rows[1:]:  # Skip header row
        cols = row.find_elements(By.TAG_NAME, "td")
        data.append([col.text for col in cols])

    return pd.DataFrame(data, columns=headers) if data else None

# Function to handle pagination and extract data from all pages
def extract_all_pages():
    all_data = []
    
    while True:
        # Extract data from the current page
        df = extract_table_data()
        if df is not None:
            all_data.append(df)
            print(all_data[-1])

        # Check if there is a "Next" button and click it
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, ".pager__item.pager__item--next a")
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(2)  # Wait for the next page to load
            WebDriverWait(driver, 1).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        except Exception as e:
            print("No more pages or error:", e)
            break

    return pd.concat(all_data, ignore_index=False) if all_data else None

# Loop through all options in the data_type dropdown menu
all_dataframes = []
for option in range(len(data_type_dropdown.options)):
    name = select_option_and_apply(driver, option)

    # Extract data from all pages for the selected option
    combined_df = extract_all_pages()
    if combined_df is not None:
        combined_df.to_csv(f"unprocessed_data/treasury_rates_{name}.csv", index=False)
        all_dataframes.append(combined_df)

# Close the browser
driver.quit()
