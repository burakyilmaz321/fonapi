"""
This script crawls data from Tefas and saves it to S3.

Usage

$ python crawler.py --start-date STARTDATE --end-date ENDDATE

or

$ python crawler.py --start-date DATE

"""
# pylint: disable=missing-function-docstring

import argparse
import json
import logging
import os
import sys
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def get_logger():
    """
    Return a logger for with console and file handler.
    Create log folder if not exists.
    """
    log_dir = os.path.join(os.path.dirname(__file__), "log")
    latest_log = 0
    try:
        log_files = [f for f in os.listdir(log_dir) if f.endswith(".log")]
        latest_log = max([int(f.split(".log")[0]) for f in log_files])
    except FileNotFoundError:
        os.mkdir(log_dir)
    except ValueError:
        pass
    log_file = os.path.join(log_dir, f"{str(latest_log + 1)}.log")
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s]: %(message)s")
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    log.addHandler(console_handler)
    return log


LOG = get_logger()
JQUERY_CLICK = "jQuery('#{}').click();".format

ROOT_URL = "https://www.tefas.gov.tr/TarihselVeriler.aspx"
MAIN_VIEW_ID = "MainContent_GridViewGenel"
DETAIL_VIEW_ID = "MainContent_GridViewDagilim"
NEXT_BUTTON_ID = "MainContent_ImageButtonGenelNext"
PAGE_NUM_ID = "MainContent_LabelGenelPageNumber"
START_DATE_ID = "MainContent_TextBoxStartDate"
END_DATE_ID = "MainContent_TextBoxEndDate"
SEARCH_BUTTON_ID = "MainContent_ButtonSearchDates"
FIRST_ROW_XPATH = '//*[@id="MainContent_GridViewGenel"]/tbody/tr[2]/td[1]'


def parse_table(content):
    """Parse an html tbody object. Return every row as a dict."""
    soup = BeautifulSoup(content, features="html.parser")
    data = []
    table = soup.find("tbody")
    rows = table.find_all("tr")
    header = rows.pop(0).find_all("th")
    header = [ele.text.strip() for ele in header]
    for row in rows:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        data.append(dict(zip(header, cols)))
    return data


def parse_pages(driver):
    """Iterate over pages and parse content."""
    LOG.info("Start")
    wait = WebDriverWait(driver, 60)
    pages = []
    while True:
        LOG.info(f"Page #{len(pages)}")
        tabs = {}
        for view in {MAIN_VIEW_ID, DETAIL_VIEW_ID}:
            elem = driver.find_element_by_id(view)
            content = elem.get_attribute("innerHTML")
            parsed = parse_table(content)
            tabs[view] = parsed
        pages.append(tabs)
        next_button = driver.find_elements_by_id(NEXT_BUTTON_ID)
        if next_button:
            # selenium WebDriver click() does not work here for some reason
            driver.execute_script(JQUERY_CLICK(NEXT_BUTTON_ID))
            # wait until the next page is loaded
            wait.until(
                EC.text_to_be_present_in_element(
                    (By.ID, PAGE_NUM_ID), str(len(pages) + 1)
                )
            )
        else:
            break
    LOG.info("End")
    return pages


def main():
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Crawls price data")
    parser.add_argument(
        "--start-date",
        help="first date to crawl. format should be DD.MM.YYYY",
        dest="start_date",
        required=True,
    )
    parser.add_argument(
        "--end-date",
        help="last date to crawl. format should be DD.MM.YYYY",
        dest="end_date",
        required=False,
    )
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date or start_date

    # validate date format
    try:
        _ = datetime.strptime(start_date, "%d.%m.%Y")
        _ = datetime.strptime(end_date, "%d.%m.%Y")
    except ValueError:
        raise ValueError("Incorrect data format, should be DD.MM.YYYY")

    assert start_date <= end_date, "start date should be before end date"

    # start crawling
    LOG.info(f"Crawling from {start_date} to {end_date}")
    LOG.info("Connecting to the driver...")
    start = datetime.now()
    with webdriver.Safari() as driver:
        driver.get(ROOT_URL)
        driver.find_element_by_id(START_DATE_ID).send_keys(start_date)
        driver.find_element_by_id(END_DATE_ID).send_keys(end_date)
        # selenium WebDriver click() does not work here for some reason
        driver.execute_script(JQUERY_CLICK(SEARCH_BUTTON_ID))
        WebDriverWait(driver, 60).until(
            EC.text_to_be_present_in_element((By.XPATH, FIRST_ROW_XPATH), end_date)
        )
        pages = parse_pages(driver)
    end = datetime.now()
    LOG.info(f"Crawling completed in {(end - start).total_seconds()} secs")

    with open("data.json", "w", encoding="utf-8") as outf:
        json.dump(pages, outf, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    sys.exit(main())
