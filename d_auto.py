#!/usr/bin/env python3
"""
Financial Data Automation Script
Automates the collection, processing, and storage of financial market data from multiple sources.
"""

import os
import sys
import time
import json
import logging
import tempfile
from datetime import datetime
from typing import Optional, Dict, List, Any
import traceback

# Data processing imports
import pandas as pd
import numpy as np
import requests

# PDF processing
import pdfplumber

# Web automation
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv

load_dotenv()

# Database
from sqlalchemy import create_engine


class Config:
    def __init__(self, report_date: Optional[str] = None):
        self.report_date = report_date or datetime.today().strftime("%d/%m/%Y")
        
        # Use a temporary directory for downloads instead of a hardcoded Windows path
        self.download_dir = tempfile.mkdtemp()
        self.calyx_username = os.getenv("CALYX_USERNAME")
        self.calyx_password = os.getenv("CALYX_PASSWORD")
        
        # Database config using environment variables
        self.db_config = {
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT", "3306"),
            'database': os.getenv("DB_NAME")
        }
        
        self.calyx_base_url = "https://online.calyxsec.com/home/"
        self.tradingview_url = "https://screener-facade.tradingview.com/screener-facade/api/v1/screener-table/scan"
        self.ngnmarket_url = "https://www.ngnmarket.com/api/companies"

class Logger:
    """Logging configuration."""
    
    @staticmethod
    def setup_logging():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('financial_automation.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(__name__)


class CalyxReportDownloader:
    """Handles automated downloading of Calyx reports."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        
    def setup_chrome_driver(self, headless: bool = True) -> webdriver.Chrome:
        """Set up Chrome driver for Docker/Cloud environments."""
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        prefs = {
            "download.default_directory": self.config.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True  # Important for PDF links
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        
        # CRITICAL for Headless Chrome: Enable downloads
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": self.config.download_dir
        })
        
        return driver
    
    def download_report(self, report_date: Optional[str] = None, headless: bool = True) -> bool:
        """Download Calyx report for the specified date."""
        if report_date is None:
            report_date = self.config.report_date
            
        # Ensure download directory exists
        os.makedirs(self.config.download_dir, exist_ok=True)
        
        driver = None
        wait = None
        try:
            self.logger.info(f"Starting Calyx report download for date: {report_date}")
            driver = self.setup_chrome_driver(headless)
            wait = WebDriverWait(driver, 30)
            
            # Login
            self._login(driver, wait)
            
            # Navigate to reports
            self._navigate_to_reports(driver, wait)
            
            # Set date and generate report
            self._generate_report(driver, wait, report_date)
            
            # Export report
            self._export_report(driver, wait)
            
            self.logger.info("✅ Report download completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error downloading report: {e}")
            self.logger.error(traceback.format_exc())
            
            # Check if PDF was downloaded despite the error
            if self._check_if_pdf_exists():
                self.logger.info("✅ PDF file found in folder despite error - continuing...")
                return True
            else:
                return False
            
        finally:
            if driver and wait:
                try:
                    self._logout_and_cleanup(driver, wait)
                except:
                    # If cleanup fails, just quit the driver
                    if driver:
                        driver.quit()
    
    def _check_if_pdf_exists(self) -> bool:
        """Check if any PDF file exists in the download directory."""
        try:
            if os.path.exists(self.config.download_dir):
                pdf_files = [f for f in os.listdir(self.config.download_dir) if f.lower().endswith('.pdf')]
                return len(pdf_files) > 0
            return False
        except:
            return False
    
    def _login(self, driver: webdriver.Chrome, wait: WebDriverWait):
        """Handle login process."""
        driver.get(self.config.calyx_base_url)
        wait.until(EC.presence_of_element_located((By.ID, "un"))).send_keys(self.config.calyx_username)
        pw = wait.until(EC.presence_of_element_located((By.ID, "pw")))
        pw.send_keys(self.config.calyx_password + Keys.RETURN)
        self.logger.info("🔐 Login completed")
    
    def _navigate_to_reports(self, driver: webdriver.Chrome, wait: WebDriverWait):
        """Navigate to reports section."""
        driver.switch_to.default_content()
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href,'stockmktreports_panel.jsp')]"))).click()
        
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, "ffrm_page")))
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'More Reports')]"))).click()
        
        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "stockmktrptpanel_reports_other_page")))
        self.logger.info("📊 Navigated to reports section")
    
    def _generate_report(self, driver: webdriver.Chrome, wait: WebDriverWait, report_date: str):
        """Set date and generate report."""
        on_radio = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='radio' and @value='on']")))
        driver.execute_script("arguments[0].click();", on_radio)
        
        ondate = wait.until(EC.presence_of_element_located((By.ID, "ondate")))
        driver.execute_script("""
            arguments[0].removeAttribute('readonly');
            arguments[0].value = arguments[1];
            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
        """, ondate, report_date)
        
        gen_btn = wait.until(EC.element_to_be_clickable((By.ID, "genreportbtn")))
        driver.execute_script("arguments[0].click();", gen_btn)
        self.logger.info(f"📅 Report generated for date: {report_date}")
    
        def _export_report(self, driver: webdriver.Chrome, wait: WebDriverWait):
            """Export report as PDF."""
            self.logger.info("⏳ Loading Report Viewer...")
            driver.switch_to.default_content()
            wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "launch_report_0_page")))
            
            export_icon = wait.until(EC.element_to_be_clickable((By.ID, "export")))
            driver.execute_script("arguments[0].click();", export_icon)
            
            self.logger.info("⏳ Switching to Export Dialog...")
            driver.switch_to.default_content()
            wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "birtrpt_export_dlg_page")))
            
            self.logger.info("📄 Selecting PDF format...")
            fmt_dropdown = wait.until(EC.presence_of_element_located((By.ID, "fmt")))
            select = Select(fmt_dropdown)
            select.select_by_value("pdf")
            
            self.logger.info(f"💾 Starting download to: {self.config.download_dir}")
            
            # Click OK button - may fail after click because dialog closes, but download starts
            try:
                ok_btn = wait.until(EC.element_to_be_clickable((By.ID, "ok")))
                driver.execute_script("arguments[0].click();", ok_btn)
            except Exception as e:
                # This is expected - the dialog closes after clicking OK
                self.logger.info("✅ Download initiated (dialog closed)")
            
            # Wait for download to complete
            self.logger.info("⏳ Waiting for download to complete...")
            time.sleep(10)  # Give it time to start
        
    def _logout_and_cleanup(self, driver: webdriver.Chrome, wait: WebDriverWait):
        """Logout and cleanup driver."""
        self.logger.info("🔒 Logging out...")
        try:
            driver.switch_to.default_content()
            driver.execute_script("window.warnOnClose = false;")
            logout = wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'logoutUser')]")))
            driver.execute_script("arguments[0].click();", logout)
            time.sleep(3)
        except:
            try:
                driver.get(f"{self.config.calyx_base_url}logoutUser")
                time.sleep(2)
            except:
                pass
        finally:
            driver.quit()


class PDFDataExtractor:
    """Extracts data from PDF reports."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def extract_nge_data(self, pdf_path: str, report_date: str) -> pd.DataFrame:
        """Extract NGE data from PDF report."""
        try:
            self.logger.info(f"📖 Extracting data from PDF: {pdf_path}")
            all_data = []
            report_date_obj = datetime.strptime(report_date, "%d/%m/%Y").date()
            
            columns = ["S/N", "Symbol", "PClose", "Open", "High", "Low",
                      "Close", "Change", "%_Change", "Deals", "Volume", "Value", "VWAP"]
            
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    table = page.extract_table()
                    if table:
                        for row in table:
                            if row and row[0] and str(row[0]).isdigit():
                                all_data.append(row)
            
            df = pd.DataFrame(all_data, columns=columns)
            df["Date"] = report_date_obj
            
            # Clean numeric columns
            numeric_cols = ["PClose", "Open", "High", "Low", "Close", "Change",
                           "%_Change", "Deals", "Volume", "Value", "VWAP"]
            
            for col in numeric_cols:
                df[col] = (df[col].astype(str)
                          .str.replace(',', '', regex=False)
                          .replace('', np.nan))
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Rename columns for consistency
            df.rename(columns={"%_Change": "change_percent"}, inplace=True)
            
            self.logger.info(f"✅ Extracted {len(df)} records from PDF")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error extracting PDF data: {e}")
            raise


class TradingViewDataFetcher:
    """Fetches data from TradingView API."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def fetch_data(self) -> pd.DataFrame:
        """Fetch TradingView data."""
        try:
            self.logger.info("📡 Fetching TradingView data...")
            
            url = self.config.tradingview_url
            params = ("?id=stocks_market_movers.all_stocks"
                     "&version=47"
                     "&columnset_id=overview"
                     "&market=nigeria")
            
            columns = ["close", "change", "volume", "relative_volume_10d_calc",
                      "market_cap_basic", "price_earnings_ttm", "earnings_per_share_diluted_ttm",
                      "earnings_per_share_diluted_growth_ttm_yoy", "dividends_yield_current", "sector"]
            
            payload = {
                "lang": "en",
                "columns": columns,
                "range": [0, 200]
            }
            
            headers = {
                "content-type": "text/plain;charset=UTF-8",
                "accept": "application/json, text/plain, */*",
                "referer": "https://www.tradingview.com/"
            }
            
            response = requests.post(url + params, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            df = self._parse_tradingview_data(data)
            self.logger.info(f"✅ Fetched {len(df)} TradingView records")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching TradingView data: {e}")
            raise
    
    def _parse_tradingview_data(self, data: Dict) -> pd.DataFrame:
        """Parse TradingView API response."""
        rows = []
        data_by_id = {item.get("id"): item.get("rawValues", []) for item in data.get("data", [])}
        
        expected_keys = {
            "TickerUniversal": "TickerUniversal",
            "Price": "Price",
            "Change": "Change",
            "Volume": "Volume",
            "RelativeVolume": "RelativeVolume",
            "MarketCap": "MarketCap",
            "PriceToEarnings": "PriceToEarnings",
            "EpsDiluted": "EpsDiluted",
            "EpsDilutedGrowth": "EpsDilutedGrowth",
            "DividendsYield": "DividendsYield",
            "Sector": "Sector",
        }
        
        ticker_data = data_by_id.get(expected_keys["TickerUniversal"])
        
        if ticker_data:
            # Extract data arrays
            data_arrays = {key: data_by_id.get(expected_keys[key], [])
                          for key in expected_keys.keys()}
            
            n = max(len(arr) for arr in data_arrays.values() if arr)
            
            def safe_get(lst, idx):
                return lst[idx] if idx < len(lst) else None
            
            for i in range(n):
                tinfo = safe_get(ticker_data, i)
                if isinstance(tinfo, dict):
                    symbol = tinfo.get("name", "")
                    description = tinfo.get("description", "")
                else:
                    symbol = str(tinfo) if tinfo is not None else ""
                    description = ""
                
                rows.append({
                    "Symbol": symbol,
                    "Description": description,
                    "Price": safe_get(data_arrays["Price"], i),
                    "Change %": safe_get(data_arrays["Change"], i),
                    "Volume": safe_get(data_arrays["Volume"], i),
                    "Rel Volume": safe_get(data_arrays["RelativeVolume"], i),
                    "Market cap": safe_get(data_arrays["MarketCap"], i),
                    "P/E": safe_get(data_arrays["PriceToEarnings"], i),
                    "EPS dil TTM": safe_get(data_arrays["EpsDiluted"], i),
                    "EPS dil growth TTM YoY": safe_get(data_arrays["EpsDilutedGrowth"], i),
                    "Div yield % TTM": safe_get(data_arrays["DividendsYield"], i),
                    "Sector": safe_get(data_arrays["Sector"], i),
                })
        else:
            # Fallback parsing
            for item in data.get("data", []):
                values = item.get("rawValues", []) or []
                
                def v(i):
                    return values[i] if i < len(values) else None
                
                rows.append({
                    "Symbol": item.get("id"),
                    "Price": v(0),
                    "Change %": v(1),
                    "Volume": v(2),
                    "Rel Volume": v(3),
                    "Market cap": v(4),
                    "P/E": v(5),
                    "EPS dil TTM": v(6),
                    "EPS dil growth TTM YoY": v(7),
                    "Div yield % TTM": v(8),
                    "Sector": v(9),
                })
        
        return pd.DataFrame(rows)


class NGNMarketDataFetcher:
    """Fetches data from NGN Market API."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def fetch_all_companies(self) -> pd.DataFrame:
        """Fetch all companies from NGN Market API."""
        try:
            self.logger.info("📡 Fetching NGN Market data...")
            
            page = 1
            limit = 100
            all_companies = []
            
            while True:
                params = {
                    "page": page,
                    "limit": limit,
                    "sort": "market_cap",
                    "order": "desc"
                }
                
                headers = {
                    "accept": "*/*",
                    "content-type": "application/json",
                    "referer": "https://www.ngnmarket.com/"
                }
                
                response = requests.get(
                    self.config.ngnmarket_url,
                    params=params,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code != 200:
                    raise Exception(f"Request failed on page {page}")
                
                data = response.json()
                companies = data.get("data", data)
                
                self.logger.info(f"Fetched page {page}, records: {len(companies)}")
                
                if not companies:
                    break
                
                all_companies.extend(companies)
                
                if len(companies) < limit:
                    break
                
                page += 1
                time.sleep(0.3)  # Rate limiting
            
            df = self._process_ngn_data(all_companies)
            self.logger.info(f"✅ Fetched {len(df)} NGN Market records")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching NGN Market data: {e}")
            raise
    
    def _process_ngn_data(self, companies: List[Dict]) -> pd.DataFrame:
        """Process NGN Market company data."""
        df = pd.DataFrame(companies)
        
        if "id" not in df.columns:
            raise ValueError("`id` column not found in company data")
        
        df.set_index("id", inplace=True)
        df.sort_index(inplace=True)
        
        # Convert numeric columns
        numeric_cols = ["sharesOutstanding", "price", "prevClose", "dayHigh", "dayLow",
                       "volume", "marketCap", "priceChange", "priceChangePercent",
                       "change7dPercent", "change52wPercent", "high52wk", "low52wk"]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Convert datetime
        if "lastUpdated" in df.columns:
            df["lastUpdated"] = pd.to_datetime(df["lastUpdated"], errors="coerce")
        
        # Rename columns for consistency
        df.rename(columns={
            "symbol": "Symbol",
            "marketCap": "Market cap",
            "sector": "Sector"
        }, inplace=True)
        
        return df


class MarketStatsDataFetcher:
    """Fetches market snapshot/stats data from NGN Market API."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.market_stats_url = "https://www.ngnmarket.com/api/market/snapshot"
    
    def fetch_market_stats(self) -> pd.DataFrame:
        """Fetch market snapshot statistics."""
        try:
            self.logger.info("📊 Fetching market stats data...")
            
            headers = {
                "accept": "application/json",
                "user-agent": "Mozilla/5.0"
            }
            
            response = requests.get(self.market_stats_url, headers=headers, timeout=30)
            response.raise_for_status()
            payload = response.json()
            
            # Safety check
            if not payload.get("success"):
                raise ValueError("API returned success = false")
            
            data = payload["data"]
            
            # Flatten marketCap nested object
            market_cap = data.pop("marketCap", {})
            flat_data = {
                **data,
                "marketCap_equity": market_cap.get("equity"),
                "marketCap_bonds": market_cap.get("bonds"),
                "marketCap_etfs": market_cap.get("etfs"),
                "marketCap_total": market_cap.get("total"),
            }
            
            # Create DataFrame
            df = pd.DataFrame([flat_data])
            
            # Convert numeric columns
            numeric_cols = ["asi", "asiChangePercent", "deals", "volume", "valueTraded",
                           "marketCap_equity", "marketCap_bonds", "marketCap_etfs", "marketCap_total"]
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Convert datetime columns
            datetime_cols = ["date", "updatedAt", "createdAt"]
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            
            self.logger.info(f"✅ Fetched market stats successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching market stats: {e}")
            raise


class DataProcessor:
    """Processes and merges data from different sources."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def merge_data(self, df_calyx: pd.DataFrame, df_tradingview: pd.DataFrame, df_ngx: pd.DataFrame) -> pd.DataFrame:
        """Merge data from all sources."""
        try:
            self.logger.info("🔄 Merging data from all sources...")
            
            # Merge TradingView data
            df_merged = df_calyx.merge(
                df_tradingview[['Symbol', 'P/E']],
                on='Symbol',
                how='left'
            )
            
            # Merge NGX data
            df_merged = df_merged.merge(
                df_ngx[['Symbol', 'Market cap', 'Sector']],
                on='Symbol',
                how='left'
            )
            
            # Clean data
            df_merged = self._clean_data(df_merged)
            
            self.logger.info(f"✅ Data merged successfully. Final dataset: {len(df_merged)} records")
            return df_merged
            
        except Exception as e:
            self.logger.error(f"❌ Error merging data: {e}")
            raise
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and filter the merged dataset."""
        initial_count = len(df)
        
        # List of specific symbols to exclude
        excluded_symbols = ['VSPBONDETF', 'VETGOODS', 'LOTUSHAL15', 'GREENWETF', 'STANBICETF30',
                           'MERVALUE', 'SIAMLETF40', 'MERGROWTH', 'VETGRIF30', 'VETINDETF',
                           'VETBANK', 'NEWGOLD']
        
        # Filter out specific excluded symbols
        df = df[~df['Symbol'].isin(excluded_symbols)]
        excluded_specific_count = initial_count - len(df)
        
        # Filter out ETFs and other unwanted symbols (general pattern)
        exclude_pattern = r'(?i)ETF|EFT|FGSUK|\d'
        df = df[~df['Symbol'].str.contains(exclude_pattern, regex=True, na=False)]
        excluded_pattern_count = (initial_count - excluded_specific_count) - len(df)
        
        # Convert date column
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        
        # Replace empty strings with NaN
        df = df.replace(r'^\s*$', np.nan, regex=True)
        
        # Log filtering results
        total_excluded = initial_count - len(df)
        self.logger.info(f"🧹 Data filtering completed:")
        self.logger.info(f"   - Initial records: {initial_count}")
        self.logger.info(f"   - Excluded specific symbols: {excluded_specific_count}")
        self.logger.info(f"   - Excluded by pattern (ETF/EFT/etc): {excluded_pattern_count}")
        self.logger.info(f"   - Total excluded: {total_excluded}")
        self.logger.info(f"   - Final records: {len(df)}")
        
        return df


class DatabaseManager:
    """Manages database operations."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.engine = None
    
    def connect(self):
        """Establish database connection."""
        try:
            connection_uri = (
                f"mysql+pymysql://{self.config.db_config['user']}:"
                f"{self.config.db_config['password']}@"
                f"{self.config.db_config['host']}:"
                f"{self.config.db_config['port']}/"
                f"{self.config.db_config['database']}"
            )
            
            self.engine = create_engine(
                connection_uri,
                connect_args={'ssl': {'ca': None}}
            )
            
            self.logger.info("🔗 Database connection established")
            
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def upload_data(self, df: pd.DataFrame, table_name: str = 'calyx_daily_data'):
        """Upload data to database with duplicate handling."""
        try:
            if self.engine is None:
                self.connect()
            
            self.logger.info(f"📤 Uploading {len(df)} records to {table_name}...")
            
            # Try to upload all data first
            try:
                df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
                self.logger.info("✅ Data uploaded successfully!")
                return
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check if it's a duplicate entry error
                if 'duplicate' in error_msg or 'integrity' in error_msg or '1062' in str(e):
                    self.logger.warning("⚠️ Duplicate entries detected, uploading row by row...")
                    self._upload_with_duplicate_skip(df, table_name)
                else:
                    # If it's a different error, raise it
                    raise
            
        except Exception as e:
            self.logger.error(f"❌ Upload failed: {e}")
            # Don't raise - log the error but continue the pipeline
            self.logger.warning("⚠️ Continuing pipeline despite upload error...")
    
    def _upload_with_duplicate_skip(self, df: pd.DataFrame, table_name: str):
        """Upload data row by row, skipping duplicates."""
        uploaded = 0
        skipped = 0
        errors = 0
        
        for idx, row in df.iterrows():
            try:
                row_df = pd.DataFrame([row])
                row_df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
                uploaded += 1
            except Exception as e:
                error_msg = str(e).lower()
                if 'duplicate' in error_msg or 'integrity' in error_msg or '1062' in str(e):
                    skipped += 1
                else:
                    errors += 1
                    self.logger.error(f"Error uploading row {idx}: {e}")
        
        self.logger.info(f"✅ Upload completed: {uploaded} uploaded, {skipped} skipped (duplicates), {errors} errors")
    
    def upload_market_stats(self, df: pd.DataFrame, table_name: str = 'market_stats'):
        """Upload market stats data to database with duplicate handling."""
        try:
            if self.engine is None:
                self.connect()
            
            self.logger.info(f"📤 Uploading market stats to {table_name}...")
            
            # Try to upload all data first
            try:
                df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
                self.logger.info("✅ Market stats uploaded successfully!")
                return
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check if it's a duplicate entry error
                if 'duplicate' in error_msg or 'integrity' in error_msg or '1062' in str(e):
                    self.logger.warning("⚠️ Duplicate market stats detected, skipping...")
                    self.logger.info("ℹ️ Market stats for this date already exists in database")
                else:
                    # If it's a different error, raise it
                    raise
            
        except Exception as e:
            self.logger.error(f"❌ Market stats upload failed: {e}")
            # Don't raise - log the error but continue the pipeline
            self.logger.warning("⚠️ Continuing pipeline despite market stats upload error...")
    

class FinancialDataAutomation:
    """Main orchestrator for the financial data automation process."""
    
    def __init__(self, report_date: Optional[str] = None):
        self.config = Config(report_date)
        self.logger = Logger.setup_logging()
        
        # Initialize components
        self.calyx_downloader = CalyxReportDownloader(self.config, self.logger)
        self.pdf_extractor = PDFDataExtractor(self.logger)
        self.tradingview_fetcher = TradingViewDataFetcher(self.config, self.logger)
        self.ngnmarket_fetcher = NGNMarketDataFetcher(self.config, self.logger)
        self.market_stats_fetcher = MarketStatsDataFetcher(self.config, self.logger)
        self.data_processor = DataProcessor(self.logger)
        self.db_manager = DatabaseManager(self.config, self.logger)
    
    def run_full_pipeline(self, download_report: bool = True, upload_to_db: bool = True, report_date: Optional[str] = None):
        """Run the complete data automation pipeline."""
        try:
            # Use provided date or fall back to config date
            processing_date = report_date or self.config.report_date
            
            self.logger.info(f"🚀 Starting Financial Data Automation Pipeline for date: {processing_date}")
            
            # Step 1: Download Calyx report (optional)
            if download_report:
                success = self.calyx_downloader.download_report(
                    report_date=processing_date,
                    headless=True
                )
                if not success:
                    self.logger.warning("⚠️ Download reported failure, but checking if PDF exists...")
            
            # Step 2: Find any PDF in folio folder and extract data
            pdf_path = self._find_any_pdf_in_folder()
            if not pdf_path:
                self.logger.error("No PDF file found in folio folder")
                return False
            
            df_calyx = self.pdf_extractor.extract_nge_data(pdf_path, processing_date)
            
            # Step 3: Fetch external data
            df_tradingview = self.tradingview_fetcher.fetch_data()
            df_ngx = self.ngnmarket_fetcher.fetch_all_companies()
            
            # Step 4: Merge and process data
            df_final = self.data_processor.merge_data(df_calyx, df_tradingview, df_ngx)
            
            # Step 5: Fetch market stats
            self.logger.info("📊 Fetching market statistics...")
            df_market_stats = self.market_stats_fetcher.fetch_market_stats()
            
            # Step 6: Upload to database (optional)
            if upload_to_db:
                self.db_manager.upload_data(df_final)
                self.db_manager.upload_market_stats(df_market_stats)
            
            # Step 7: Clean up folio folder after successful processing
            self._cleanup_folio_folder()
            
            self.logger.info("🎉 Pipeline completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _find_any_pdf_in_folder(self) -> Optional[str]:
        """Find any PDF file in the folio folder with a retry mechanism."""
        try:
            timeout = 60  # Wait up to 60 seconds for the file to appear
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                all_files = os.listdir(self.config.download_dir)
                
                # Check if Chrome is still downloading (look for .crdownload or .tmp)
                if any(f.endswith('.crdownload') for f in all_files):
                    self.logger.info("⏳ Download still in progress (.crdownload found)...")
                    time.sleep(2)
                    continue
                
                pdf_files = [f for f in all_files if f.lower().endswith('.pdf')]
                
                if pdf_files:
                    selected_pdf = pdf_files[0]
                    pdf_path = os.path.join(self.config.download_dir, selected_pdf)
                    
                    if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                        self.logger.info(f"✅ Found PDF: {selected_pdf}")
                        return pdf_path
                
                time.sleep(2)
            
            self.logger.error("❌ Timeout: No PDF files found after waiting.")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ Error finding PDF file: {e}")
            return None
    
    def _cleanup_folio_folder(self):
        """Clean up all files in the folio folder after successful processing."""
        try:
            self.logger.info(f"🧹 Cleaning up folio folder: {self.config.download_dir}")
            
            if not os.path.exists(self.config.download_dir):
                self.logger.warning("Folio folder doesn't exist, nothing to clean")
                return
            
            files_removed = 0
            for filename in os.listdir(self.config.download_dir):
                file_path = os.path.join(self.config.download_dir, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    files_removed += 1
                    self.logger.info(f"🗑️ Removed: {filename}")
            
            self.logger.info(f"✅ Cleanup completed. Removed {files_removed} files")
            
        except Exception as e:
            self.logger.error(f"❌ Error during cleanup: {e}")
            # Don't fail the whole process if cleanup fails
            pass


def main():
    """Main entry point."""
    # Example: will use current date by default
    automation = FinancialDataAutomation()
    
    # Run the full pipeline
    success = automation.run_full_pipeline(
        download_report=True,  # Set to False if you already have the PDF
        upload_to_db=True,     # Set to False if you don't want to upload to DB
        report_date="20/01/2026"  # Use DD/MM/YYYY format
    )
    
    if success:
        print("✅ Financial data automation completed successfully!")
    else:
        print("❌ Financial data automation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
