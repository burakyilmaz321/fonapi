# -*- coding: utf-8 -*-
"""
This script crawls data from Tefas and uploads it to S3.

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
import time
from datetime import datetime
from gzip import GzipFile

import boto3
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# constants
URL = "https://www.tefas.gov.tr/TarihselVeriler.aspx"
START_DATE_ID = "MainContent_TextBoxStartDate"
END_DATE_ID = "MainContent_TextBoxEndDate"
SEARCH_BUTTON_ID = "MainContent_ButtonSearchDates"
TAB_VIEWS = ["Genel", "Dagilim"]
NO_DATA_TEXT = "Veri Bulunamadı"

# env variables
S3_BUCKET = os.getenv("FONAPI_S3_BUCKET", "fonapi-staging")

# format callables
VIEW_ID = "MainContent_GridView{}".format
PAGE_ID = "MainContent_Label{}PageNumber".format
NEXT_ID = "MainContent_ImageButton{}Next".format
JQUERY_CLICK = "jQuery('#{}').click();".format
ROW_XPATH = "//*[@id='MainContent_GridView{}']/tbody/tr[2]/td[1]".format
TAB_XPATH = "//*[@id='MainContent_tabs']/ul/li[{}]".format

# chrome options
OPTIONS = Options()
OPTIONS.add_argument("--headless")


def get_logger():
    """
    Return a logger for with console handler.
    """
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s]: %(message)s")
    console_handler.setFormatter(formatter)
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


def parse_pages(driver, out_file, view):
    """Iterate over pages and parse content."""
    LOG.info("Start")
    wait = WebDriverWait(driver, 60)
    page = 1
    while True:
        LOG.info(f"Page #{page}")
        tabs = {}
        elem = driver.find_element_by_id(VIEW_ID(view))
        content = elem.get_attribute("innerHTML")
        parsed = parse_table(content)
        tabs[view] = parsed
        # write output as gziped jsonlines file
        with GzipFile(out_file, "a") as outf:
            json_str = json.dumps(tabs, ensure_ascii=False) + "\n"
            json_bytes = json_str.encode("utf-8")
            outf.write(json_bytes)
        # find the next button
        next_button = driver.find_elements_by_id(NEXT_ID(view))
        if next_button:
            driver.execute_script("arguments[0].scrollIntoView()", next_button[0])
            action = ActionChains(driver)
            action.move_to_element(next_button[0])
            action.click()
            action.perform()
            # wait until the next page is loaded
            wait.until(
                EC.text_to_be_present_in_element((By.ID, PAGE_ID(view)), str(page + 1))
            )
        else:
            break
        page += 1
    LOG.info("End")


def upload_to_s3(out_file):
    """Upload output directory to s3"""
    basename = os.path.basename(out_file)
    LOG.info(f"Uploading {out_file} to s3://{S3_BUCKET}/{basename}")
    s3_resource = boto3.resource("s3")
    obj = s3_resource.Object(S3_BUCKET, basename)
    obj.upload_file(Filename=out_file)
    LOG.info("Upload completed!")


def update_value(elem, value):
    """Update the value of an html element"""
    elem.click()
    elem.clear()
    elem.send_keys(value)
    elem.submit()
    time.sleep(10)


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
    out_dir = os.path.join(os.path.dirname(__file__), "out")
    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    # start crawling
    LOG.info(f"Crawling from {start_date} to {end_date}")
    LOG.info("Connecting to the driver...")
    start = datetime.now()
    with webdriver.Firefox(options=OPTIONS) as driver:
        driver.implicitly_wait(10)
        driver.get(URL)
        update_value(driver.find_element_by_id(START_DATE_ID), start_date)
        update_value(driver.find_element_by_id(END_DATE_ID), end_date)
        driver.find_element_by_id(SEARCH_BUTTON_ID).click()
        time.sleep(10)
        # exit if no data in the view
        if NO_DATA_TEXT in driver.page_source:
            LOG.info("No data for this date. Exiting...")
            return
        for index, view in enumerate(TAB_VIEWS):
            driver.execute_script("window.scrollTo(0,0)")
            out_file = os.path.join(
                out_dir,
                "{}-{}-{}.jsonl.gz".format(
                    start_date.replace(".", ""), end_date.replace(".", ""), view
                ),
            )
            tab = driver.find_element_by_xpath(TAB_XPATH(index + 1))
            action = ActionChains(driver)
            action.move_to_element(tab)
            action.click()
            action.perform()
            parse_pages(driver, out_file, view)
            upload_to_s3(out_file)
    end = datetime.now()
    LOG.info(f"Crawling completed in {(end - start).total_seconds()} secs")


if __name__ == "__main__":
    sys.exit(main())
