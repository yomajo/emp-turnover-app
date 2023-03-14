from extensions import db
from sqlalchemy import UniqueConstraint


class Company(db.Model):
    __tablename__ = 'company'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(280), unique=True, nullable=False)
    municipality = db.Column(db.String(60))
    classifier_id = db.Column(db.Integer, db.ForeignKey('sector.id'))
    hires = db.relationship('Hiring', backref='company')
    bookmark = db.relationship('Bookmark', backref='company')

    def __repr__(self) -> str:
        return f'Company(id={self.id}, name={self.name}, municipality={self.municipality})'


class Sector(db.Model):
    __tablename__ = 'sector'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), unique=True, nullable=False)
    companies = db.relationship('Company', backref='sector')

    def __repr__(self) -> str:
        return f'Sector(id={self.id}, name={self.name})'


class Bookmark(db.Model):
    __tablename__ = 'bookmark'
    
    id = db.Column(db.Integer, primary_key=True)
    jar_id = db.Column(db.Integer, db.ForeignKey('company.id'))

    def __repr__(self) -> str:
        return f'Bookmark(id={self.id}, jar_id={self.jar_id})'


class Hiring(db.Model):
    __tablename__ = 'hires'
    __table_args__ = (UniqueConstraint('jar_id', 'date', name='_co_date_hires'),)

    id = db.Column(db.Integer, primary_key=True)
    jar_id = db.Column(db.Integer, db.ForeignKey('company.id'))
    date = db.Column(db.DateTime, nullable=False)
    emps = db.Column(db.Integer, nullable=False)
    daily_turnover = db.Column(db.Integer)

    def __repr__(self) -> str:
        return f'Hiring(id={self.id}, jar_id={self.jar_id}, date={self.date}, emps={self.emps})'