"""
This script crawls data from Tefas and saves it as json locally.

To do so, it does:

* Starts a headless browser session with selenium
* Gets historic data page
* Enters given start and end dates
* Since the data is paginated, traverse each page until there is none
* Saves the result

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


# constants
URL = "https://www.tefas.gov.tr/TarihselVeriler.aspx"
MAIN_VIEW_ID = "MainContent_GridViewGenel"
DETAIL_VIEW_ID = "MainContent_GridViewDagilim"
PAGE_NUM_ID = "MainContent_LabelGenelPageNumber"
START_DATE_ID = "MainContent_TextBoxStartDate"
END_DATE_ID = "MainContent_TextBoxEndDate"
NEXT_BUTTON_ID = "MainContent_ImageButtonGenelNext"
SEARCH_BUTTON_ID = "MainContent_ButtonSearchDates"
FIRST_ROW_XPATH = '//*[@id="MainContent_GridViewGenel"]/tbody/tr[2]/td[1]'

# format callable that returns a javascript expression for clicking an element
JQUERY_CLICK = "jQuery('#{}').click();".format


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


def parse_pages(driver, out_dir):
    """Iterate over pages and parse content."""
    LOG.info("Start")
    wait = WebDriverWait(driver, 60)
    page = 1
    while True:
        LOG.info(f"Page #{page}")
        tabs = {}
        for view in {MAIN_VIEW_ID, DETAIL_VIEW_ID}:
            elem = driver.find_element_by_id(view)
            content = elem.get_attribute("innerHTML")
            parsed = parse_table(content)
            tabs[view] = parsed
        with open(os.path.join(out_dir, f"{page}.json"), "w", encoding="utf-8") as outf:
            json.dump(tabs, outf, ensure_ascii=False)
        next_button = driver.find_elements_by_id(NEXT_BUTTON_ID)
        if next_button:
            # selenium WebDriver click() does not work here for some reason
            driver.execute_script(JQUERY_CLICK(NEXT_BUTTON_ID))
            # wait until the next page is loaded
            wait.until(
                EC.text_to_be_present_in_element((By.ID, PAGE_NUM_ID), str(page + 1))
            )
        else:
            break
        page += 1
    LOG.info("End")


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

    # create output directory
    out_dir = os.path.join(
        os.path.dirname(__file__),
        "out",
        f"{start_date.replace('.', '')}-{end_date.replace('.', '')}",
    )
    os.makedirs(out_dir)

    # start crawling
    LOG.info(f"Crawling from {start_date} to {end_date}")
    LOG.info("Connecting to the driver...")
    start = datetime.now()
    with webdriver.Safari() as driver:
        driver.get(URL)
        driver.find_element_by_id(START_DATE_ID).send_keys(start_date)
        driver.find_element_by_id(END_DATE_ID).send_keys(end_date)
        # selenium WebDriver click() does not work here for some reason
        driver.execute_script(JQUERY_CLICK(SEARCH_BUTTON_ID))
        WebDriverWait(driver, 60).until(
            EC.text_to_be_present_in_element((By.XPATH, FIRST_ROW_XPATH), end_date)
        )
        parse_pages(driver, out_dir)
    end = datetime.now()
    LOG.info(f"Crawling completed in {(end - start).total_seconds()} secs")


if __name__ == "__main__":
    sys.exit(main())
