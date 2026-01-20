#!/usr/bin/env python3
"""
Financial Data Automation Script - Production Version
Automates collection, processing, and storage.
Optimized for GitHub Actions and Cloud Deployment.
"""

import os
import sys
import time
import json
import logging
import tempfile
import traceback
from datetime import datetime
from typing import Optional, Dict, List, Any

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

# Database
from sqlalchemy import create_engine

load_dotenv()

class Config:
    """Configuration management for the financial data automation."""
    
    def __init__(self, report_date: Optional[str] = None):
        # Default report date - use current date if none provided
        self.report_date = report_date or datetime.today().strftime("%d/%m/%Y")
        
        # Use a temporary directory for cloud environments
        self.download_dir = tempfile.mkdtemp()
        
        # Credentials from environment variables
        self.calyx_username = os.getenv("CALYX_USERNAME")
        self.calyx_password = os.getenv("CALYX_PASSWORD")
        
        # Database configuration - Fixed to properly call os.getenv
        self.db_config = {
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT", "3306"),
            'database': os.getenv("DB_NAME")
        }
        
        # API URLs
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
        """Set up Chrome driver for Linux/Cloud environments."""
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Download preferences
        prefs = {
            "download.default_directory": self.config.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
            
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
    
    def download_report(self, report_date: Optional[str] = None, headless: bool = True) -> bool:
        """Download Calyx report for the specified date."""
        if report_date is None:
            report_date = self.config.report_date
            
        os.makedirs(self.config.download_dir, exist_ok=True)
        
        driver = None
        wait = None
        try:
            self.logger.info(f"Starting Calyx report download for date: {report_date}")
            driver = self.setup_chrome_driver(headless)
            wait = WebDriverWait(driver, 45) # Increased timeout for server lag
            
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
            if self._check_if_pdf_exists():
                self.logger.info("✅ PDF file found in folder despite error - continuing...")
                return True
            return False
            
        finally:
            if driver:
                try:
                    self._logout_and_cleanup(driver, wait)
                except:
                    driver.quit()

    def _check_if_pdf_exists(self) -> bool:
        try:
            pdf_files = [f for f in os.listdir(self.config.download_dir) if f.lower().endswith('.pdf')]
            return len(pdf_files) > 0
        except:
            return False
    
    def _login(self, driver: webdriver.Chrome, wait: WebDriverWait):
        driver.get(self.config.calyx_base_url)
        wait.until(EC.presence_of_element_located((By.ID, "un"))).send_keys(self.config.calyx_username)
        pw = wait.until(EC.presence_of_element_located((By.ID, "pw")))
        pw.send_keys(self.config.calyx_password + Keys.RETURN)
        self.logger.info("🔐 Login completed")
    
    def _navigate_to_reports(self, driver: webdriver.Chrome, wait: WebDriverWait):
        driver.switch_to.default_content()
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href,'stockmktreports_panel.jsp')]"))).click()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, "ffrm_page")))
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'More Reports')]"))).click()
        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "stockmktrptpanel_reports_other_page")))
        self.logger.info("📊 Navigated to reports section")
    
    def _generate_report(self, driver: webdriver.Chrome, wait: WebDriverWait, report_date: str):
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
        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "launch_report_0_page")))
        export_icon = wait.until(EC.element_to_be_clickable((By.ID, "export")))
        driver.execute_script("arguments[0].click();", export_icon)
        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "birtrpt_export_dlg_page")))
        fmt_dropdown = wait.until(EC.presence_of_element_located((By.ID, "fmt")))
        Select(fmt_dropdown).select_by_value("pdf")
        ok_btn = wait.until(EC.element_to_be_clickable((By.ID, "ok")))
        driver.execute_script("arguments[0].click();", ok_btn)
        time.sleep(20) 
    
    def _logout_and_cleanup(self, driver: webdriver.Chrome, wait: WebDriverWait):
        try:
            driver.switch_to.default_content()
            driver.execute_script("window.warnOnClose = false;")
            logout = wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'logoutUser')]")))
            driver.execute_script("arguments[0].click();", logout)
            time.sleep(2)
        finally:
            driver.quit()


class PDFDataExtractor:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def extract_nge_data(self, pdf_path: str, report_date: str) -> pd.DataFrame:
        try:
            self.logger.info(f"📖 Extracting data from PDF: {pdf_path}")
            all_data = []
            report_date_obj = datetime.strptime(report_date, "%d/%m/%Y").date()
            columns = ["S/N", "Symbol", "PClose", "Open", "High", "Low", "Close", "Change", "%_Change", "Deals", "Volume", "Value", "VWAP"]
            
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    table = page.extract_table()
                    if table:
                        for row in table:
                            if row and row[0] and str(row[0]).isdigit():
                                all_data.append(row)
            
            df = pd.DataFrame(all_data, columns=columns)
            df["Date"] = report_date_obj
            numeric_cols = ["PClose", "Open", "High", "Low", "Close", "Change", "%_Change", "Deals", "Volume", "Value", "VWAP"]
            for col in numeric_cols:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False).replace('', np.nan)
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df.rename(columns={"%_Change": "change_percent"}, inplace=True)
            return df
        except Exception as e:
            self.logger.error(f"❌ PDF Extraction Error: {e}")
            raise


class TradingViewDataFetcher:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        
    def fetch_data(self) -> pd.DataFrame:
        try:
            url = self.config.tradingview_url
            params = "?id=stocks_market_movers.all_stocks&version=47&columnset_id=overview&market=nigeria"
            columns = ["close", "change", "volume", "relative_volume_10d_calc", "market_cap_basic", "price_earnings_ttm", "sector"]
            payload = {"lang": "en", "columns": columns, "range": [0, 300]}
            headers = {"content-type": "text/plain;charset=UTF-8", "referer": "https://www.tradingview.com/"}
            
            response = requests.post(url + params, json=payload, headers=headers, timeout=30)
            data = response.json()
            
            rows = []
            for item in data.get("data", []):
                v = item.get("rawValues", [])
                rows.append({
                    "Symbol": item.get("d", [""])[0].split(":")[1] if ":" in str(item.get("d")) else item.get("id"),
                    "P/E": v[5] if len(v) > 5 else None
                })
            return pd.DataFrame(rows)
        except Exception as e:
            self.logger.error(f"TradingView Error: {e}")
            return pd.DataFrame(columns=['Symbol', 'P/E'])


class NGNMarketDataFetcher:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def fetch_all_companies(self) -> pd.DataFrame:
        try:
            response = requests.get(self.config.ngnmarket_url, params={"limit": 500}, timeout=30)
            data = response.json()
            companies = data.get("data", [])
            df = pd.DataFrame(companies)
            df.rename(columns={"symbol": "Symbol", "marketCap": "Market cap", "sector": "Sector"}, inplace=True)
            return df
        except Exception as e:
            self.logger.error(f"NGN Market Error: {e}")
            return pd.DataFrame(columns=['Symbol', 'Market cap', 'Sector'])


class MarketStatsDataFetcher:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.market_stats_url = "https://www.ngnmarket.com/api/market/snapshot"
    
    def fetch_market_stats(self) -> pd.DataFrame:
        response = requests.get(self.market_stats_url, timeout=30)
        payload = response.json()["data"]
        market_cap = payload.pop("marketCap", {})
        flat_data = {**payload, "marketCap_equity": market_cap.get("equity"), "marketCap_total": market_cap.get("total")}
        df = pd.DataFrame([flat_data])
        df["date"] = pd.to_datetime(df["date"])
        return df


class DataProcessor:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def merge_data(self, df_calyx: pd.DataFrame, df_tradingview: pd.DataFrame, df_ngx: pd.DataFrame) -> pd.DataFrame:
        df_merged = df_calyx.merge(df_tradingview[['Symbol', 'P/E']], on='Symbol', how='left')
        df_merged = df_merged.merge(df_ngx[['Symbol', 'Market cap', 'Sector']], on='Symbol', how='left')
        
        excluded_symbols = ['VSPBONDETF', 'NEWGOLD', 'VETBANK']
        df_merged = df_merged[~df_merged['Symbol'].isin(excluded_symbols)]
        df_merged = df_merged[~df_merged['Symbol'].str.contains(r'ETF|EFT|FGSUK|\d', regex=True, na=False)]
        
        return df_merged


class DatabaseManager:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.engine = None
    
    def connect(self):
        conn_uri = f"mysql+pymysql://{self.config.db_config['user']}:{self.config.db_config['password']}@{self.config.db_config['host']}:{self.config.db_config['port']}/{self.config.db_config['database']}"
        # Added common SSL requirement for Render/PlanetScale/AWS
        self.engine = create_engine(conn_uri, connect_args={'ssl': {'ca': None}} if os.getenv("DB_SSL") else {})
        self.logger.info("🔗 Database connection established")
    
    def upload_data(self, df: pd.DataFrame, table_name: str = 'calyx_daily_data'):
        if self.engine is None: self.connect()
        try:
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            self.logger.info(f"✅ Uploaded {len(df)} records")
        except:
            self.logger.warning("⚠️ Duplicates found, skipping row by row...")
            for _, row in df.iterrows():
                try: pd.DataFrame([row]).to_sql(table_name, con=self.engine, if_exists='append', index=False)
                except: pass

    def upload_market_stats(self, df: pd.DataFrame, table_name: str = 'market_stats'):
        if self.engine is None: self.connect()
        try: df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
        except: pass


class FinancialDataAutomation:
    def __init__(self, report_date: Optional[str] = None):
        self.config = Config(report_date)
        self.logger = Logger.setup_logging()
        self.calyx_downloader = CalyxReportDownloader(self.config, self.logger)
        self.pdf_extractor = PDFDataExtractor(self.logger)
        self.tradingview_fetcher = TradingViewDataFetcher(self.config, self.logger)
        self.ngnmarket_fetcher = NGNMarketDataFetcher(self.config, self.logger)
        self.market_stats_fetcher = MarketStatsDataFetcher(self.config, self.logger)
        self.data_processor = DataProcessor(self.logger)
        self.db_manager = DatabaseManager(self.config, self.logger)
    
    def run_full_pipeline(self, download_report: bool = True, upload_to_db: bool = True):
        try:
            proc_date = self.config.report_date
            self.logger.info(f"🚀 Pipeline Start: {proc_date}")
            
            if download_report:
                self.calyx_downloader.download_report(report_date=proc_date)
            
            pdf_files = [f for f in os.listdir(self.config.download_dir) if f.lower().endswith('.pdf')]
            if not pdf_files:
                self.logger.error("No PDF found")
                return False
                
            pdf_path = os.path.join(self.config.download_dir, pdf_files[0])
            df_calyx = self.pdf_extractor.extract_nge_data(pdf_path, proc_date)
            df_tv = self.tradingview_fetcher.fetch_data()
            df_ngx = self.ngnmarket_fetcher.fetch_all_companies()
            
            df_final = self.data_processor.merge_data(df_calyx, df_tv, df_ngx)
            df_stats = self.market_stats_fetcher.fetch_market_stats()
            
            if upload_to_db:
                self.db_manager.upload_data(df_final)
                self.db_manager.upload_market_stats(df_stats)
            
            self.logger.info("🎉 Success!")
            return True
        except Exception as e:
            self.logger.error(f"❌ Pipeline Failed: {e}")
            traceback.print_exc()
            return False


def main():
    automation = FinancialDataAutomation()
    # Runs for today's date by default
    success = automation.run_full_pipeline(download_report=True, upload_to_db=True)
    if not success: sys.exit(1)

if __name__ == "__main__":
    main()
