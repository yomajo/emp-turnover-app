import os


class Config():
    SECRET_KEY = os.getenv('SECRET_KEY')
    SQLALCHEMY_DATABASE_URI = 'sqlite:///companies.db'
    STATS_SINCE = '2020-01'     # Min available '2018-01'
    DELIMITER = ';'
    COMPANIES_OF_INTEREST = [
        'devbridge',
        'maxima',
    ]