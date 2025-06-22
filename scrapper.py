# Import Library
from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from datetime import date, datetime
import pandas as pd
import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import stat

# Fungsi Setup Driver
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Error setting up driver: {e}")
        print("Pastikan ChromeDriver sudah terinstall dan ada di PATH")
        return None

# Fungsi Perbaikan Permission Direktori
def fix_directory_permissions(directory_path):
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"Direktori dibuat: {directory_path}")
        current_permissions = os.stat(directory_path).st_mode
        os.chmod(directory_path, current_permissions | stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
        print(f"Permission direktori diperbaiki: {directory_path}")
    except Exception as e:
        print(f"Error memperbaiki permission: {e}")

# Fungsi Tunggu Baris Baru
def wait_for_rows_to_load(driver, previous_row_count, timeout=15):
    print(f"Waiting for new rows to load. Previous count: {previous_row_count}. Timeout: {timeout}s")
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, "tr[data-rowkey]")) > previous_row_count or \
                      len(d.find_elements(By.CSS_SELECTOR, "tr.row-RdUXZpkv")) > previous_row_count
        )
        current_rows = driver.find_elements(By.CSS_SELECTOR, "tr[data-rowkey]")
        if not current_rows:
            current_rows = driver.find_elements(By.CSS_SELECTOR, "tr.row-RdUXZpkv")
        print(f"New rows loaded. Current count: {len(current_rows)}")
        return True
    except TimeoutException:
        print("Timeout waiting for new rows to load. No new rows detected or count did not increase.")
        return False
    except StaleElementReferenceException:
        print("StaleElementReferenceException while waiting for rows, retrying might be needed or DOM changed too fast.")
        return False

# Fungsi Utama Scraping
def tradingviewUSStocksScrapper():
    root_path = r'D:\Big Data Predictive Analytics\FinalProjects\tradingview-us-stocks'
    fix_directory_permissions(root_path)
    url = 'https://www.tradingview.com/screener/' 
    driver = setup_driver()
    if not driver:
        return
    
    try:
        print("Mengakses TradingView Stock Screener...")
        driver.get(url)
        
        # Tunggu halaman awal
        print("Waiting for initial page load and first table data...")
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tr[data-rowkey], tr.row-RdUXZpkv"))
            )
            time.sleep(3)
            print("Initial page and table data loaded.")
        except TimeoutException:
            print("Timeout menunggu elemen tabel awal. Periksa koneksi atau struktur halaman.")
            html_on_fail = driver.page_source
            with open('debug_initial_table_fail.html', 'w', encoding='utf-8') as f:
                f.write(html_on_fail)
            print("HTML page at failure (initial table) saved as debug_initial_table_fail.html")
            return
        
        MAX_ROWS_TARGET = 3000
        MIN_ROWS_TARGET = 1000 
        SCROLL_PAUSE_TIME_AFTER_LOAD = 1.5
        MAX_UNCHANGED_SCROLLS = 30
        WAIT_FOR_NEW_ROWS_TIMEOUT = 30
        processed_row_identifiers = set()
        all_parsed_data = []
        headers = [
            'Symbol', 'Company_Name', 'Price', 'Change_%', 'Volume', 
            'Rel_Volume', 'Market_Cap', 'P/E', 'E1'
            'PS_Growth_%', 
            'Div_Yield_%', 'Sector'
        ]
        
        # Cari container scrollable
        scrollable_container = None
        try:
            potential_containers = driver.find_elements(By.XPATH, "//div[contains(@class, 'table__overflow-wrapper') or contains(@class, 'tv-screener-table__pane') or contains(@class, 'list-container')]")
            for pc in potential_containers:
                if pc.value_of_css_property('overflow-y') in ['auto', 'scroll']:
                    scrollable_container = pc
                    print(f"Found specific scrollable container: {pc.get_attribute('class')}")
                    break
            if not scrollable_container:
                tables = driver.find_elements(By.TAG_NAME, "table")
                for table_el in tables:
                    parent_div = table_el.find_element(By.XPATH, "./ancestor::div[1]")
                    if parent_div.value_of_css_property('overflow-y') in ['auto', 'scroll'] and parent_div.size['height'] > 0:
                        scrollable_container = parent_div
                        print(f"Found parent div of table as scrollable container: {parent_div.get_attribute('class')}")
                        break
        except Exception as e:
            print(f"Could not find scrollable container: {e}")
        
        unchanged_scroll_attempts = 0
        while len(all_parsed_data) < MAX_ROWS_TARGET:
            previous_unique_row_count_in_set = len(processed_row_identifiers)
            current_page_html = driver.page_source
            soup = BeautifulSoup(current_page_html, "html.parser")
            dom_rows_elements = soup.find_all('tr', {'data-rowkey': True})
            if not dom_rows_elements:
                dom_rows_elements = soup.find_all('tr', class_='row-RdUXZpkv')
            
            new_rows_parsed_this_iteration = 0
            for row_element in dom_rows_elements:
                row_key_attr = row_element.get('data-rowkey')
                current_row_identifier = row_key_attr if row_key_attr else "".join([td.get_text(strip=True)[:20] for td in row_element.find_all('td')[:3]])
                if not current_row_identifier or current_row_identifier in processed_row_identifiers:
                    continue
                
                try:
                    parsed_cell_values = []
                    cells = row_element.find_all('td')
                    if not cells or len(cells) < 2:
                        continue
                    
                    # Parsing Simbol
                    symbol = 'N/A'
                    first_cell_content = cells[0]
                    symbol_link = first_cell_content.find('a', class_='tv-screener__symbol') or first_cell_content.find('a', href=lambda x: x and '/symbols/' in x)
                    if symbol_link:
                        symbol = symbol_link.get_text(strip=True)
                    else:
                        potential_symbol_span = first_cell_content.find('span', class_=lambda x: x and 'ticker' in x.lower())
                        if potential_symbol_span:
                            symbol = potential_symbol_span.get_text(strip=True)
                        elif first_cell_content.find('a'):
                            symbol = first_cell_content.find('a').get_text(strip=True)
                        else:
                            symbol = first_cell_content.get_text(strip=True).split('\n')[0]
                    parsed_cell_values.append(symbol)
                    
                    # Parsing Nama Perusahaan
                    company_name = 'N/A'
                    name_span = first_cell_content.find('span', class_=lambda x: x and ('description' in x.lower() or 'title' in x.lower()))
                    if name_span:
                        company_name = name_span.get_text(strip=True)
                    if company_name == 'N/A' and len(cells) > 1:
                        second_cell_content = cells[1]
                        name_span_alt = second_cell_content.find('span', class_=lambda x: x and ('description' in x.lower() or 'title' in x.lower()))
                        if name_span_alt:
                            company_name = name_span_alt.get_text(strip=True)
                        elif name_span_alt is None and not any(char.isdigit() for char in second_cell_content.get_text(strip=True)):
                            company_name = second_cell_content.get_text(strip=True)
                    if symbol == 'N/A' and company_name == 'N/A':
                        continue
                    if company_name == 'N/A' and symbol != 'N/A' and len(cells) > 1:
                        name_candidate_text = cells[1].get_text(strip=True)
                        if name_candidate_text and not name_candidate_text.replace('.', '', 1).replace('-', '', 1).isdigit() and '%' not in name_candidate_text:
                            company_name = name_candidate_text
                    parsed_cell_values.append(company_name)
                    
                    # Parsing Data Numerik
                    for header_idx in range(2, len(headers)):
                        cell_data_idx = header_idx
                        if cell_data_idx < len(cells):
                            cell_text = cells[cell_data_idx].get_text(strip=True).replace('\u2014', '').replace('—', '').strip()
                            parsed_cell_values.append(cell_text if cell_text else 'N/A')
                        else:
                            parsed_cell_values.append('N/A')
                    
                    while len(parsed_cell_values) < len(headers):
                        parsed_cell_values.append('N/A')
                    final_row_data = parsed_cell_values[:len(headers)]
                    all_parsed_data.append(final_row_data)
                    processed_row_identifiers.add(current_row_identifier)
                    new_rows_parsed_this_iteration += 1
                
                except Exception as e:
                    print(f"Error parsing row: {e}")
                    continue
            
            print(f"Parsed {new_rows_parsed_this_iteration} new unique rows this iteration. Total unique data: {len(all_parsed_data)}")
            if len(all_parsed_data) >= MAX_ROWS_TARGET:
                print(f"Target {MAX_ROWS_TARGET} rows reached. Stopping.")
                break
            
            # Deteksi perubahan data
            if new_rows_parsed_this_iteration > 0:
                unchanged_scroll_attempts = 0
            else:
                current_dom_row_elements_count = len(driver.find_elements(By.CSS_SELECTOR, "tr[data-rowkey], tr.row-RdUXZpkv"))
                if current_dom_row_elements_count <= previous_unique_row_count_in_set + 5:
                    unchanged_scroll_attempts += 1
                else:
                    unchanged_scroll_attempts = 0
            
            print(f"Unchanged scroll attempts: {unchanged_scroll_attempts}/{MAX_UNCHANGED_SCROLLS}")
            if unchanged_scroll_attempts >= MAX_UNCHANGED_SCROLLS:
                print("No new data loaded after multiple scroll attempts.")
                break
            
            # Scroll
            print("Scrolling down...")
            scroll_target = scrollable_container if scrollable_container else driver.find_element(By.TAG_NAME, 'body')
            if scrollable_container:
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", scroll_target)
            else:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Tunggu data baru
            print(f"Waiting up to {WAIT_FOR_NEW_ROWS_TIMEOUT}s for new rows...")
            time.sleep(2)
            rows_loaded_after_scroll = False
            try:
                wait_start_time = time.time()
                while time.time() - wait_start_time < WAIT_FOR_NEW_ROWS_TIMEOUT:
                    current_row_elements_in_dom = driver.find_elements(By.CSS_SELECTOR, "tr[data-rowkey], tr.row-RdUXZpkv")
                    if len(current_row_elements_in_dom) > previous_unique_row_count_in_set + new_rows_parsed_this_iteration:
                        print(f"Detected increase in DOM row elements ({len(current_row_elements_in_dom)}).")
                        rows_loaded_after_scroll = True
                        break
                    time.sleep(0.5)
                if not rows_loaded_after_scroll:
                    print("No significant increase in DOM row elements after scroll.")
            except Exception as e:
                print(f"Exception during dynamic wait: {e}")
            
            time.sleep(SCROLL_PAUSE_TIME_AFTER_LOAD)
        
        if not all_parsed_data:
            print("Tidak ada data yang berhasil diekstrak.")
            final_html_debug = driver.page_source
            with open('debug_final_page_no_data_v2.html', 'w', encoding='utf-8') as f:
                f.write(final_html_debug)
            print("Final page HTML saved for debugging.")
            return
        
        df = pd.DataFrame(all_parsed_data, columns=headers)
        df = df[df['Symbol'] != 'N/A']
        df = df.drop_duplicates(subset=['Symbol'], keep='first')
        print(f"Total {len(df)} unique saham berhasil di-scraping.")
        if 0 < len(df) < MIN_ROWS_TARGET:
            print(f"PERINGATAN: Jumlah saham ({len(df)}) kurang dari target minimal ({MIN_ROWS_TARGET}).")
        save_data_to_file(df, root_path, 'TradingView_US_Stocks_Extended_v2')
    
    except Exception as e:
        print(f"Error signifikan dalam proses scraping: {e}")
        try:
            if driver:
                html_on_error = driver.page_source
                with open('debug_general_error_v2.html', 'w', encoding='utf-8') as f:
                    f.write(html_on_error)
                print("HTML page at error saved as debug_general_error_v2.html")
        except Exception as e_save_err:
            print(f"Could not save debug HTML on error: {e_save_err}")
    finally:
        if driver:
            driver.quit()
        print("Browser ditutup")

# Fungsi Simpan Data
def save_data_to_file(df, root_path, base_filename):
    if df.empty:
        print("DataFrame kosong, tidak ada data untuk disimpan.")
        return
    today = date.today().strftime('%y-%m-%d')
    timestamp = datetime.now().strftime('%H%M%S')
    for attempt in range(3):
        try:
            filename = f'{base_filename}_{today}_{timestamp}.xlsx'
            if attempt > 0:
                filename = f'{base_filename}_{today}_{timestamp}_attempt{attempt+1}.xlsx'
            excel_path = os.path.join(root_path, filename)
            df.to_excel(excel_path, index=False, engine='openpyxl')
            print(f"✅ Data berhasil disimpan sebagai Excel: {excel_path}")
            break
        except PermissionError:
            print(f"❌ Attempt {attempt+1}: Permission error Excel. File mungkin terbuka.")
            if attempt == 2:
                print("Gagal Excel karena PermissionError. Mencoba CSV.")
                try:
                    csv_filename = f'{base_filename}_{today}_{timestamp}.csv'
                    csv_path = os.path.join(root_path, csv_filename)
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    print(f"✅ Data disimpan sebagai CSV: {csv_path}")
                except Exception as e_csv:
                    print(f"❌ Gagal menyimpan sebagai CSV: {e_csv}")
            else:
                time.sleep(2)
        except Exception as e:
            print(f"❌ Error saving Excel: {e}")
            break
    print("\n📊 Preview Data (DataFrame):")
    print(df.head(10))
    print(f"\n📈 Summary: {len(df)} saham (setelah cleaning).")
    if 'Sector' in df.columns and not df.empty:
        sector_counts = df['Sector'].value_counts().head(10)
        print(f"\n🏢 Top 10 Sektor:")
        for sector, count in sector_counts.items():
            print(f"  {sector}: {count} saham")

# Fungsi Test Lingkungan
def test_environment():
    print("🔍 Testing environment...")
    test_dir = r'D:\Big Data Predictive Analytics\Web Scrapping\test_scraper_dir_v2'
    try:
        if not os.path.exists(test_dir): os.makedirs(test_dir)
        test_df = pd.DataFrame({'test_col': [1, 2, 3]})
        test_file = os.path.join(test_dir, 'test_output.xlsx')
        test_df.to_excel(test_file, index=False)
        os.remove(test_file)
        os.rmdir(test_dir)
        print("✅ File access test: PASSED")
    except Exception as e:
        print(f"❌ File access test: FAILED - {e}")
    print("Testing Selenium setup...")
    driver_test = None
    try:
        driver_test = setup_driver()
        if driver_test:
            print("✅ Selenium driver setup: PASSED")
            driver_test.get("https://www.google.com") 
            print(f"✅ Selenium basic navigation: PASSED (Page title: {driver_test.title})")
        else:
            print("❌ Selenium driver setup: FAILED (driver is None)")
    except Exception as e:
        print(f"❌ Selenium test: FAILED - {e}")
    finally:
        if driver_test: driver_test.quit()

# Main Execution
if __name__ == "__main__":
    print("🚀 TradingView US Stocks Scraper v2")
    print("=" * 50)
    test_environment()
    print("\n⚠️  PENTING:")
    print("1. ChromeDriver terinstall & versi sesuai Google Chrome.")
    print("2. ChromeDriver di PATH atau folder skrip.")
    print("3. Tutup file Excel target (hindari PermissionError).")
    print("4. Koneksi internet stabil.")
    print("5. Proses scraping bisa lama.")
    choice = input("\nPilih method scraping:\n1. Selenium (Recommended)\n2. Basic requests (Limited)\n3. Test saja\nPilihan (1-3): ")
    if choice == "1":
        tradingviewUSStocksScrapper()
    elif choice == "2":
        tradingviewBasicScrapper()
    elif choice == "3":
        print("Test environment selesai.")
    else:
        print("Pilihan tidak valid. Menjalankan Selenium (default).")
        tradingviewUSStocksScrapper()
    print("\n✅ Proses selesai!")