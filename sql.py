import sqlalchemy as sql 
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import Date, Table, Column, Integer, String,\
                       MetaData, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base


from flask import Flask, jsonify
from datetime import datetime

DeclarativeBase = declarative_base()


class Reviews(DeclarativeBase):
    __tablename__ = "reviews"

    review_id = Column(String(100), primary_key=True)
    star_rating = Column(Integer)
    helpful_votes = Column(Integer)
    total_votes = Column(Integer)
    vine = Column(Text(1))
    verified_purchase = Column(Text(1))
    review_headline = Column(Text(100))
    review_body = Column(Text(500))
    review_date = Column(Date)

    def _asdict(self):
        return {c.key: getattr(self, c.key)
                       for c in sql.inspect(self).mapper.column_attrs}

class Customers(DeclarativeBase):
    __tablename__ = "customers"
    customer_id = Column(Integer, primary_key=True)

    def _asdict(self):
        return {c.key: getattr(self, c.key)
                       for c in sql.inspect(self).mapper.column_attrs}


class Products(DeclarativeBase):
    __tablename__ = "products"

    product_id = Column(String(20), primary_key=True)
    product_parent = Column(Integer)
    product_title = Column(Text(100))
    product_category = Column(Text(100))
    marketplace = Column(String(30))

    def _asdict(self):
        return {c.key: getattr(self, c.key)
                       for c in sql.inspect(self).mapper.column_attrs}

class ForeignKeys(DeclarativeBase):
    __tablename__ = "foreign_keys"

    id = Column(Integer, primary_key=True)
    review_id = Column(String(100), ForeignKey("reviews.review_id"))
    customer_id = Column(Integer, ForeignKey("customers.customer_id"))
    product_id = Column(String(10), ForeignKey("products.product_id"))

    reviews = relationship("Reviews")
    customers = relationship("Customers")
    products = relationship("Products")

    # def _asdict(self):
    #     return {c.key: getattr(self, c.key)
    #                    for c in sql.inspect(self).mapper.column_attrs}


engine = sql.create_engine('mysql://root:root@localhost:3306/amazon')
connection = engine.connect()
# DeclarativeBase.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

app = Flask(__name__)

def get():
    return session.query(Customers, Reviews, Products, ForeignKeys).\
                    join(Reviews, Customers, Products)

def my_jsonify(query):
    return jsonify([dict(row[1]._asdict(), **dict(row[0]._asdict(), **row[2]._asdict()))  for row in query])
def get_products(query):
    return jsonify([dict(row[1]._asdict()) for row in query])
def get_customers(query):
    return jsonify([row[0]._asdict() for row in query])
def get_reviews(query):
    return jsonify([row[2]._asdict() for row in query])

@app.route("/product_id/<id_>", methods=["GET"]) # 1
def get_product(id_):
    query = get().filter_by(**{"product_id":id_}).all()
    return get_products(query)

@app.route("/product_id/<id_>/star_rating/<rating>", methods=["GET"]) # 2
def get_product_rating(id_, rating):
    query = get().filter(sql.and_(Reviews.star_rating==rating, ForeignKeys.product_id==id_))
    return get_products(query)

@app.route("/customer_id/<id_>", methods=["GET"]) # 3
def get_customer(id_):
    query = get().filter(Customers.customer_id==id_).all()
    return get_reviews(query)

@app.route("/n_most_items/<n>/<start>/<end>", methods=["GET"]) # 4
def get_n(n,start, end):
    start = datetime.strptime(start, "%d-%m-%Y")
    end = datetime.strptime(end, "%d-%m-%Y")
    query = get().group_by(Products.product_id).filter(Reviews.review_date.between(start, end)).order_by(sql.func.count(Reviews.review_id).desc()).limit(n).all()
    return get_products(query)

@app.route("/n_most_customers/<n>/<start>/<end>", methods=["GET"]) # 5
def get_n_cust(n,start,end):
    start = datetime.strptime(start, "%d-%m-%Y")
    end = datetime.strptime(end, "%d-%m-%Y")
    query = get().filter(sql.and_(Reviews.review_date.between(start, end), Reviews.verified_purchase=="Y")).group_by(Customers.customer_id).order_by(sql.func.count(Customers.customer_id).desc()).limit(n).all()
    return get_customers(query)

@app.route("/n_best_fraction/<n>", methods=["GET"]) # 6
def get_n_frac(n):
    query = session.query(ForeignKeys, sql.func.count(ForeignKeys.product_id)).\
                    join(Products, Reviews).\
                    filter(Reviews.star_rating==5).\
                    group_by(ForeignKeys.product_id).\
                    having(
                        sql.func.count(ForeignKeys.product_id) > 100
                    ).all()
    return get_products(query)


@app.route("/n_productive/<n>/<start>/<end>/<is_hater>")
def get_hater_or_lover(n, start, end, is_hater): # 7
    start = datetime.strptime(start, "%d-%m-%Y")
    end = datetime.strptime(end, "%d-%m-%Y")
    if is_hater == "true":
        query = get().filter(sql.and_(Reviews.review_date.between(start, end), sql.or_(Reviews.star_rating==4, Reviews.star_rating==5))).group_by(Customers.customer_id).order_by(sql.func.count(Reviews.review_id).desc()).limit(n).all()
        return get_customers(query)
    elif is_hater == "false":
        query = get().filter(sql.and_(Reviews.review_date.between(start, end), sql.or_(Reviews.star_rating==1, Reviews.star_rating==2))).group_by(Customers.customer_id).order_by(sql.func.count(Reviews.review_id).desc()).limit(n).all()
        return get_customers(query)
    else:
        return "Please specify is that query for hater(true, false)"

if __name__ == "__main__":
    app.run()