import os
import zipfile
import logging
from io import BytesIO
from datetime import date, datetime
from time import perf_counter, sleep
from flask import current_app
from sqlalchemy.exc import IntegrityError
import requests
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

from models import Company, Sector, Hiring
from extensions import db


TEMPLATE_URL = 'https://atvira.sodra.lt/imones/downloads/YYYY/daily-YYYY-MM.csv.zip'
SRC_DIR = 'src'
BLOCK_SIZE = 8192
COL_RENAME_MAPPING = {
    'Juridinių asmenų registro kodas (jarCode)': 'jar_id',
    'Pavadinimas (name)': 'name',
    'Savivaldybė, kurioje registruota(municipality)': 'municipality',
    'Ekonominės veiklos rūšies kodas(ecoActCode)': 'sector_id',
    'Ekonominės veiklos rūšies pavadinimas(ecoActName)': 'sector_name',
    'Data (date)': 'date',
    'Apdraustųjų skaičius (numInsured)': 'emps',
}
HIRES_COLS = ['jar_id', 'name', 'date', 'emps']


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
    sleep(r.elapsed.total_seconds() + 2)
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
    '''downloads datasets, parses, adds select companies data to database
    returns True if no errors were raised'''
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
                previous_file = os.path.join(SRC_DIR, csv_fname)
                # skip download if file already exists
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
        except Exception as e:
            log.exception(f'There was a problem collecting/processing data. Err: {e}')
            return False

    log.info(f'Database for select companies filled in {perf_counter()-start_all:.2f} seconds')
    return True


def process_csv(csv_fpath: str, companies_of_interest: list) -> None:
    '''cleans / adds csv data to database tables'''
    df = get_select_cols_df(csv_fpath)
    all_sectors = df[['sector_id', 'sector_name']].drop_duplicates('sector_id')
    add_sectors_to_db(all_sectors)
    all_companies = df[['jar_id', 'name', 'municipality', 'sector_id',]].drop_duplicates('jar_id')
    add_companies_to_db(all_companies)
    tgt_companies = pull_target_companies_df(df, companies_of_interest)
    split_by_company_add_hires_data_to_db(tgt_companies)


def get_select_cols_df(csv_fpath: str) -> pd.DataFrame:
    '''returns dataframe of passed csv w/ filtered and renamed columns'''
    df_raw = pd.read_csv(csv_fpath, delimiter=current_app.config['DELIMITER'])
    working = df_raw[['Juridinių asmenų registro kodas (jarCode)',
        'Pavadinimas (name)', 'Savivaldybė, kurioje registruota(municipality)',
        'Ekonominės veiklos rūšies kodas(ecoActCode)',
        'Ekonominės veiklos rūšies pavadinimas(ecoActName)', 'Data (date)',
        'Apdraustųjų skaičius (numInsured)',]]
    working.rename(columns=COL_RENAME_MAPPING, inplace=True)
    working['date'] = pd.to_datetime(working['date'], format='%Y-%m-%d')
    working.loc[~np.isfinite(working['jar_id']), 'jar_id'] = -1
    working['jar_id'] = working['jar_id'].astype(int, copy=False)
    return working


def add_sectors_to_db(sectors: pd.DataFrame) -> None:
    '''iterates over rows to adds sectors to database'''
    log.info(f'Adding sectors...')
    for _, row in sectors.iterrows():
        sector = Sector(id=row['sector_id'], name=row['sector_name'])
        try:
            db.session.add(sector)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            continue


def add_companies_to_db(companies: pd.DataFrame) -> None:
    '''iterates over rows to adds companies to database'''
    log.info(f'Checking {len(companies.index):,} companies against current db entries, adding...')
    for _, row in companies.iterrows():
        co = Company(id=row['jar_id'], name=row['name'],
            municipality=row['municipality'], classifier_id=row['sector_id'],)
        try:
            db.session.add(co)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            continue


def pull_target_companies_df(df: pd.DataFrame, companies_of_interest: list) -> pd.DataFrame:
    '''returns dataframe for all found companies'''
    log.info(f'Pulling data for companies: {companies_of_interest}')
    jar_ids_of_interest = []
    tgt_companies = df[HIRES_COLS]
    for search_co in companies_of_interest:
        matches = tgt_companies['name'].str.contains(search_co, case=False)
        matches_df = tgt_companies[matches]
        jar_ids_of_interest.extend(list(matches_df['jar_id'].unique()))
    # filter companies
    return tgt_companies[tgt_companies['jar_id'].isin(jar_ids_of_interest)]


def split_by_company_add_hires_data_to_db(df: pd.DataFrame) -> None:
    '''adds Hiring data for each unique company in df'''
    log.info(f'Adding employee data for each company...')
    unique_jar_ids = list(df['jar_id'].unique())
    for jar_id in unique_jar_ids:
        co_df = df[df['jar_id'] == jar_id]
        co_by_date = co_df.sort_values(by=['date'], ascending=True)
        co_by_date['daily_turnover'] = co_by_date['emps'].diff().fillna(0).astype(int)
        add_co_hiring_data(co_by_date)


def add_co_hiring_data(df: pd.DataFrame) -> None:
    '''iterates over dataframe rows and adds company hiring data to database table Hiring'''
    ref_day = None
    for _, row in df.iterrows():
        # convert pd.TimeStamp to datetime
        pd_ts: pd.Timestamp = row['date']
        dt = datetime(year=pd_ts.year, month=pd_ts.month, day=pd_ts.day)

        # df is sorted by date, ref_day = first day of month, where daily_turnover cant be calculated from current df
        if not ref_day:
            ref_day = dt
            jar_id = row['jar_id']
            emps_start_of_month = row['emps']

        hires = Hiring(jar_id=row['jar_id'], date=dt, emps=row['emps'], daily_turnover=row['daily_turnover'],)
        try:
            db.session.add(hires)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            continue
    attempt_add_turnover_for_mo_start(jar_id, ref_day, emps_start_of_month)


def attempt_add_turnover_for_mo_start(jar_id: int, ref_day: datetime, emps_start_of_month: int) -> None:
    '''csv data contains data for month only. Attempt to pull data from last month last day and calc
    emp turnover on current month first day'''
    try:
        day_b4 = ref_day - relativedelta(days=1)
        co_entries_subq = db.session.query(Hiring).filter(Hiring.jar_id==jar_id)
        day_b4_hiring_data: Hiring = co_entries_subq.filter(Hiring.date==day_b4).one()
        tgt_hiring_data: Hiring = co_entries_subq.filter(Hiring.date==ref_day).one()
        tgt_hiring_data.daily_turnover = day_b4_hiring_data.emps - tgt_hiring_data.emps
        db.session.commit()
    except Exception as e:
        log_date = ref_day.strftime('%Y-%m-%d')
        log.debug(f'Failed to fix first day of month emp turnover of co id: {jar_id}, date: {log_date}. Err: {e}')


if __name__ == "__main__":
    pass