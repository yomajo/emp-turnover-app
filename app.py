import logging
from logging import Formatter
from logging.handlers import RotatingFileHandler

from flask import current_app, Flask, render_template, redirect, url_for, flash
from sqlalchemy.exc import OperationalError
import pandas as pd

from config import Config
from extensions import db
from models import Company, Sector, Hiring, Bookmark
from data_loader import load_data_to_database


# setup app, extensions, logging
app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)

file_handler = RotatingFileHandler('app.log', maxBytes=1_000_000, backupCount=1)
formatter = Formatter(
    '%(levelname)s:%(asctime)s:%(funcName)s: %(message)s\n',
    datefmt='%y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
app.logger.addHandler(file_handler)
log = logging.getLogger(__name__)


@app.route('/')
def index():
    show_setup, show_load_data = False, False
    try:
        co = db.session.query(Company).first()
        if not co:
            flash('Database ready, Load data', category='is-warning')
            show_load_data = True
    except OperationalError:
        flash('Setup database first!', category='is-danger')
        show_setup = True
    return render_template('index.html', show_setup=show_setup, show_load_data=show_load_data)


@app.route('/setup_db')
def setup_db():
    db.create_all()
    log.info('database file and tables created')
    flash('Database File And Tables Created', category='is-success')
    return redirect(url_for('index'))


@app.route('/load_data')
def load_data():
    load_successful = load_data_to_database(current_app.config['COMPANIES_OF_INTEREST'])
    if load_successful:
        return 'All good. Inspect sqlite database'
    return 'Processing errors. Check logs'


@app.route('/companies')
def companies():
    all_companies = db.session.query(Company.id).count()
    data_companies_ids = db.session.query(Hiring.jar_id).distinct()
    data_companies = db.session.query(Company).filter(Company.id.in_(data_companies_ids)).all()
    return render_template('companies.html', all_companies=all_companies, data_companies=data_companies)


if __name__ == "__main__":
    pass