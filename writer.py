from sqlalchemy.orm import sessionmaker 
from sqlalchemy import MetaData
import sqlalchemy as sql

import pandas as pd 
import numpy as np

from sql import Reviews, Customers, Products, ForeignKeys
meta = MetaData()
engine = sql.create_engine('mysql://root:root@localhost:3306/amazon?charset=utf8mb4')
connection = engine.connect()

Session = sessionmaker(bind=engine)
session = Session()

if __name__ == "__main__":
	df = pd.read_csv("amazon_reviews.tsv", sep="\t", header=0, error_bad_lines=False, encoding="utf-8")
	df.dropna(inplace=True)
	
	products_keys = ["product_id", 'product_parent',\
					'product_title', "product_category",\
					"marketplace"]
	reviews_keys = ["review_id", 'helpful_votes',\
					'total_votes', "verified_purchase",\
					"review_headline", "review_body",\
					"review_date",\
					"vine", "star_rating"]

	customer_key = "customer_id"


	unique_product = dict()
	unique_customers = dict()

	for inx,data in df.iterrows():
		if data[customer_key] not in unique_customers.keys() and data["product_id"] not in unique_product.keys():
			prod = Products(**{key:data[key] for key in products_keys})
			cust = Customers(**{"customer_id":data[customer_key]}) 
		elif data[customer_key] in unique_customers.keys() and data["product_id"] in unique_product.keys():
			cust = unique_customers[data[customer_key]]
			prod = unique_product[data["product_id"]]
		elif data[customer_key] in unique_customers.keys():
			cust = unique_customers[data[customer_key]]
			prod = Products(**{key:data[key] for key in products_keys})
		else:
			cust = Customers(**{"customer_id":data[customer_key]}) 
			prod = unique_product[data["product_id"]]

		unique_product[data["product_id"]] = prod 
		unique_customers[data[customer_key]] = cust

		session.add(prod)
		session.add(cust)
		rev = Reviews(**{key:data[key] for key in reviews_keys})
		rel = ForeignKeys(review_id=rev.review_id, customer_id=cust.customer_id, product_id=prod.product_id)
		session.add(rev)
		session.add(rel)

	session.commit()

# ALTER TABLE products CONVERT TO CHARACTER SET utf8mb4;
