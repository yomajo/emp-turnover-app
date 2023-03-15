import logging
from logging import Formatter
from logging.handlers import RotatingFileHandler
from datetime import datetime

from flask import current_app, request, Flask, render_template, redirect, url_for, flash
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
    log.info(f'Load data started...')
    load_successful = load_data_to_database(current_app.config['COMPANIES_OF_INTEREST'])    
    if load_successful:
        flash(f'Companies, sectors data has been loaded and now available for analysis', category='is-success')
        return redirect(url_for('companies'))
    flash('Error occurred when downloading / processing data. Check logs', category='is-danger')
    return redirect(url_for('index'))


@app.route('/companies')
def companies():
    try:
        all_companies = db.session.query(Company.id).count()
        data_companies_ids = db.session.query(Hiring.jar_id).distinct()
        data_companies = db.session.query(Company).filter(Company.id.in_(data_companies_ids)).all()
        return render_template('companies.html', all_companies=all_companies, data_companies=data_companies)
    except OperationalError as e:
        log.exception(f'Cant access companies. Database not setup? Err: {e}')
        flash('Database not setup', category='is-danger')
        return redirect(url_for('index'))


@app.route('/company/<int:id>', methods=['GET', 'POST'])
def company(id):
    co = db.session.query(Company).get_or_404(id)
    base_q = db.session.query(Hiring.date, Hiring.emps, Hiring.daily_turnover).filter(Hiring.jar_id==id)

    if request.method == 'POST':
        start_dt = datetime.strptime(request.form['start_date'], '%Y-%m-%d')
        end_dt = datetime.strptime(request.form['end_date'], '%Y-%m-%d')
        # validate form data before querying database
        if end_dt < start_dt:
            flash('Selected end date, is earlier than period start date', category='is-warning')
            return redirect(url_for('company', id=id))
        # apply date filtering
        base_q = base_q.filter(Hiring.date.between(start_dt, end_dt))

    data = base_q.order_by(Hiring.date).all()
    dates = [tup[0].strftime('%Y-%m-%d') for tup in data]
    emps = [tup[1] for tup in data]
    daily_turnover = [tup[2] for tup in data]

    hires = sum(filter(lambda x: x>0, daily_turnover))
    layoffs = abs(sum(filter(lambda x: x<0, daily_turnover)))
    net_change_pct = round((sum(daily_turnover) / emps[0])*100, 2)
    ctx = {
        'offer_end_date': datetime.today().strftime('%Y-%m-%d'),
        'offer_start_date': dates[0],
        'co': co,
        'dates': dates,
        'emps': emps,
        'daily_turnover': daily_turnover,
        'hires': hires,
        'layoffs': layoffs,
        'net_change_pct': net_change_pct}
    return render_template('company.html', **ctx)


@app.route('/update')
def update():
    flash('Update currently not implemented', category='is-danger')
    return redirect(url_for('index'))


if __name__ == "__main__":
    pass