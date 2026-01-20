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
import pymysql


class Config:
    """Configuration management for the financial data automation."""
    
    def __init__(self, report_date: Optional[str] = None):
        # Default report date - use current date if none provided
        self.report_date = report_date or datetime.today().strftime("%d/%m/%Y")
        
        # Use temporary directory for downloads
        self.download_dir = tempfile.mkdtemp(prefix="calyx_reports_")
        
        # Calyx credentials from environment variables
        self.calyx_username = os.getenv("CALYX_USERNAME")
        self.calyx_password = os.getenv("CALYX_PASSWORD")
        
        # Database configuration
        self.db_config = {
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT"),
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
        """Set up Chrome driver with appropriate options."""
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Download preferences
        prefs = {
            "download.default_directory": os.path.abspath(self.config.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        if headless:
            chrome_options.add_argument("--headless=new")
            
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
    
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
        ok_btn = wait.until(EC.element_to_be_clickable((By.ID, "ok")))
        driver.execute_script("arguments[0].click();", ok_btn)
        
        # Simple wait for download to complete
        self.logger.info("⏳ Waiting 20 seconds for download to complete...")
        time.sleep(20)
        self.logger.info("✅ Download wait completed")
    
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
            
            # Step 2: Find any PDF in temp folder and extract data
            pdf_path = self._find_any_pdf_in_folder()
            if not pdf_path:
                self.logger.error("No PDF file found in temp folder")
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
            
            # Step 7: Clean up temp folder after successful processing
            self._cleanup_temp_folder()
            
            self.logger.info("🎉 Pipeline completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _find_any_pdf_in_folder(self) -> Optional[str]:
        """Find any PDF file in the temp folder."""
        try:
            self.logger.info(f"🔍 Looking for any PDF in: {self.config.download_dir}")
            
            if not os.path.exists(self.config.download_dir):
                self.logger.error(f"Temp folder doesn't exist: {self.config.download_dir}")
                return None
            
            # Get all PDF files
            all_files = os.listdir(self.config.download_dir)
            pdf_files = [f for f in all_files if f.lower().endswith('.pdf')]
            
            self.logger.info(f"📄 Found {len(pdf_files)} PDF files: {pdf_files}")
            
            if not pdf_files:
                self.logger.error("❌ No PDF files found in temp folder")
                return None
            
            # Use the first PDF file found
            selected_pdf = pdf_files[0]
            pdf_path = os.path.join(self.config.download_dir, selected_pdf)
            
            # Verify file exists and has content
            if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                self.logger.info(f"✅ Using PDF file: {selected_pdf}")
                self.logger.info(f"📊 File size: {os.path.getsize(pdf_path)} bytes")
                return pdf_path
            else:
                self.logger.error(f"❌ PDF file is empty or doesn't exist: {selected_pdf}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error finding PDF file: {e}")
            return None
    
    def _cleanup_temp_folder(self):
        """Clean up all files in the temp folder after successful processing."""
        try:
            self.logger.info(f"🧹 Cleaning up temp folder: {self.config.download_dir}")
            
            if not os.path.exists(self.config.download_dir):
                self.logger.warning("Temp folder doesn't exist, nothing to clean")
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


def main():
    """Main entry point."""
    # Use current date by default
    automation = FinancialDataAutomation()
    
    # Run the full pipeline
    success = automation.run_full_pipeline(
        download_report=True,
        upload_to_db=True,
        report_date=None  # Use current date
    )
    
    if success:
        print("✅ Financial data automation completed successfully!")
    else:
        print("❌ Financial data automation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()