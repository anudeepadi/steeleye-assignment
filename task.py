# imports

import os
import json
import boto3
import requests
import bs4
import logging
import zipfile
import sys
import csv
import xml.etree.cElementTree as ET

from copy import deepcopy
from datetime import datetime
from csv import DictWriter, DictReader
from pathlib import Path
from typing import Union, List


maxInt = sys.maxsize
while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

# set up logging
LOGGER = "MAIN"
TMP_DIR = "tmp"

# create logger
logger = logging.getLogger(LOGGER)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

# create tmp directory
Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

def get_links_from_url(url: str) -> List[str]:
    """Return a list of links from url"""
    logger.info("Getting links from %s", url)
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.text, "xml")
    links = [link.text for link in soup.find_all('str', {'name': 'download_link'})]
    return links

def download_zip(url: str, filename: str) -> None:
    """Download zip file from url"""
    try:
        logger.info("Downloading %s", url)
        response = requests.get(url)
        with open(filename, "wb") as f:
            f.write(response.content)
    except Exception as e:
        logger.error("Error downloading %s", url)
        logger.error(e)

def extract_zip(filename: str) -> None:
    """Extract zip file to tmp directory"""
    try:
        logger.info("Extracting %s", filename)
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extractall(TMP_DIR)
    except zipfile.BadZipFile:
        logger.error("Error extracting %s", filename)

def get_xml_files() -> List[str]:
    """Return a list of xml files"""
    return [os.path.join(TMP_DIR, filename) for filename in os.listdir(TMP_DIR) if filename.endswith(".xml")]
    
def to_csv(filename: str) -> None:
    """Write data to csv file"""
    tags = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]
    xml_files = get_xml_files()
    csv_path = os.path.join(TMP_DIR, filename)
    with open(csv_path, "w", encoding="utf8", newline="") as f:
        writer = DictWriter(f, fieldnames=tags)
        writer.writeheader()
        for xml_file in xml_files:
            parse(xml_file, tags, writer)

def parse(xml_file: Path, tags: List[str], csv_collector: DictWriter, max_row: int = None) -> None:
    """
    Parse xml file and write to csv file    
    """

    main_tag = [tag.split(".")[0] if len(tag.split(".")) > 1 else tag for tag in tags]
    values_template = {tag: "" for tag in tags}

    context = ET.iterparse(xml_file, events=("start", "end"))

    values = deepcopy(values_template)

    parent = None
    __parent = []
    import re
    count = 0
    start_time = datetime.utcnow()
    for event, elem in context:
        tag = re.sub("{.*}", "", elem.tag) if re.match("{.*}", elem.tag) else elem.tag
        text = elem.text

        if event == "start":
            __parent.append(tag)
        elif event == "end":
            __parent.pop(-1)

        if tag in tags and text:
            values[tag] = text
            continue

        if len(__parent) > 1 and f"{__parent[-2]}.{tag}" in tags and text:
            values[f"{__parent[-2]}.{tag}"] = text
            continue

        if event == "end" and len(__parent) > 0 and f"{__parent[-1]}.{tag}" in tags and text:
            values[f"{__parent[-1]}.{tag}"] = text

        if parent is None and tag in main_tag:
            parent = __parent[-1]

        if event == 'end' and tag == parent:
            count += 1
            logging.getLogger(LOGGER).debug(f"Row collected {values}")
            csv_collector.writerow(values)
            values = deepcopy(values_template)
        elem.clear()

        if max_row is not None and isinstance(max_row, int) and count >= max_row:
            logging.getLogger(LOGGER).info(f"Max row({max_row}) hit, breaking.")
            break
    logging.getLogger(LOGGER).info(f"Data collected in {(datetime.utcnow() - start_time).seconds} seconds.")
    return

def csv_to_aws(csv_path: str, bucket: str, key: str) -> None:
    """Upload csv to aws s3 bucket"""
    try:
        s3 = boto3.resource("s3")
        s3.meta.client.upload_file(csv_path, bucket, key)
        logger.info("Uploaded %s to %s/%s", csv_path, bucket, key)
    except Exception as e:
        logger.error("Error uploading %s to %s/%s", csv_path, bucket, key)
        logger.error(e)

def lambda_handler(event, context):
    """Lambda handler"""
    if event:
        logger.info("Event: %s", event)
    if context:
        logger.info("Context: %s", context)
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }   


def main() -> None:
    """Main function"""
    # get links from url   
    links = get_links_from_url("https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100")
    for link in links:
        filename = os.path.join(TMP_DIR, link.split("/")[-1])
        download_zip(link, filename)
        extract_zip(filename)
    to_csv("output.csv")
    csv_to_aws(os.path.join(TMP_DIR, "output.csv"), "bucket-name", "output.csv")

if __name__ == "__main__":
    event = None
    context = None
    lambda_handler(event, context)