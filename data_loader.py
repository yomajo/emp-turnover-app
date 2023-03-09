import os
import zipfile
import logging
from io import BytesIO
from datetime import date
from time import perf_counter, sleep
from flask import current_app
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

from models import Company, Sector, Hiring
from extensions import db


TEMPLATE_URL = 'https://atvira.sodra.lt/imones/downloads/YYYY/daily-YYYY-MM.csv.zip'
SRC_DIR = 'src'
BLOCK_SIZE = 8192
log = logging.getLogger('app')
log.setLevel(logging.DEBUG)


class NotReleasedError(Exception):
    pass


def setup_src_dir():
    '''adds src dir if not exists'''
    if not os.path.exists(SRC_DIR):
        os.makedirs(SRC_DIR)
        log.info(f'Created directory ./{SRC_DIR}/ for csv files')


def month_strs(since_date: date) -> str:
    '''yields YYYY-MM strings up to today starting with arg: since_date'''
    today_date = date.today()
    until_date = date(year=today_date.year, month=today_date.month, day=1) - relativedelta(months=1)
    if until_date < since_date:
        raise ValueError(f'since_date ({since_date}) is higher than last month ({until_date})')
    incr_date = since_date
    yield incr_date.strftime('%Y-%m')     # include start date
    while incr_date < until_date:
        incr_date = incr_date + relativedelta(months=1)
        yield incr_date.strftime('%Y-%m')


def get_csv(url: str, csv_fname: str) -> str:
    '''builds url, downloads zip to in-memory zip file, extracts to SRC_DIR and returns csv path'''    
    # download zip to in-memory zip file
    r = requests.get(url, stream=True)
    log.debug(f'Previous request took: {r.elapsed}. Giving rest for remote server...')
    sleep(r.elapsed + 2)
    if not r.ok:
        raise NotReleasedError(f'File at {url} not yet available')
    buffer = BytesIO()
    for data in r.iter_content(BLOCK_SIZE):
        buffer.write(data)

    # Extract the archived CSV file to the 'src' folder
    with zipfile.ZipFile(buffer) as zip_file:
        zip_file.extract(csv_fname, SRC_DIR)
    return os.path.join(SRC_DIR, csv_fname)


def get_start_date() -> date:
    '''returns date obj from date str in app config'''
    since_year, since_mo = current_app.config['STATS_SINCE'].split('-')
    start_date = date(year=int(since_year), month=int(since_mo), day=1)
    return start_date


def load_data_to_database(companies_of_interest: list) -> bool:
    '''downloads datasets, parses, adds select companies data to database'''
    start_all = perf_counter()
    setup_src_dir()
    start_date = get_start_date()
    previous_file = None
    for dt_str in month_strs(start_date):
        start_this = perf_counter()
        try:
            year, month = dt_str.split('-')
            csv_fname = f'daily-{year}-{month}.csv'
            if os.path.exists(os.path.join(SRC_DIR, csv_fname)):
                log.warning(f'File {csv_fname} alrady downloaded, processing...')
                process_csv(os.path.join(SRC_DIR, csv_fname), companies_of_interest)
                continue
            url = TEMPLATE_URL.replace('YYYY', year).replace('MM', month)
            csv_path = get_csv(url, csv_fname)
            log.debug(f'Downloaded: {csv_path} in {perf_counter()-start_this:.2f} seconds, processing...')
            process_csv(os.path.join(SRC_DIR, csv_fname), companies_of_interest)
            
            # cleanup files
            if previous_file:
                log.debug(f'Deleting processed data file: {previous_file}')
                # os.remove(previous_file)
                log.warning('ACTUAL DELETE SUSPENDED during dev')

            previous_file = os.path.join(SRC_DIR, csv_fname)
        except NotReleasedError as e:
            log.warning(f'Err: {e}')

    log.info(f'Database for select companies filled in {perf_counter()-start_all:.2f} seconds')
    return True


def process_csv(csv_fpath: str, companies_of_interest: list) -> None:
    '''cleans / adds csv data to database tables'''
    pass



if __name__ == "__main__":
    pass