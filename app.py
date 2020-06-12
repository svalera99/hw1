from flask import Flask, jsonify, request, abort, Response
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('bigdata')
session.row_factory = dict_factory

app = Flask(__name__)


@app.route('/', methods=["GET"])
def defaultHello():
    return 'Hello, world'

@app.route('/<string:tableName>/<string:id>/', methods=["GET"])
def getDataInTheTableById(tableName, id):
    tableId = tableName.split("_")[-1] + "_id"
    data, res = session.execute("SELECT * FROM {} WHERE {}='{}' AND review_date>'1900-01-01' ALLOW FILTERING".format(tableName, tableId, id)), []
    for i in data:
        dictRow = dict(i)
        dictRow['review_date'] = str(dictRow['review_date'])
        res.append(dictRow)
    if len(res) != 0:
        return jsonify(res)
    return {"message": "No data for the specific id."}


@app.route('/product/<string:id>/<string:starRating>/', methods=['GET'])
def getProductWithRating(id, starRating):
    data, res = session.execute("SELECT * FROM reviews_by_product WHERE product_id='{}' AND star_rating='{}' AND review_date>'1900-09-09' ALLOW FILTERING".format(id, starRating)), []
    for i in data:
        dictRow = dict(i)
        dictRow['review_date'] = str(dictRow['review_date'])
        res.append(dictRow)
    if len(res) != 0:
        return jsonify(res)
    return {"message": "No data for the specific id and star rating."}



@app.route('/product/<int:num>/<string:startDate>/<string:endDate>/')
def getAllProductsFilteredByDate(num, startDate, endDate):
    data = session.execute("SELECT product_id, product_title FROM reviews_by_product, reivew_date WHERE review_date>='{}' AND review_date<='{}' ALLOW FILTERING".format(startDate, endDate))
    products, ids = dict(), dict()
    for i in data:
        dictRow = dict(i)
        dictRow["review_date"] = str(dictRow["review_date"])
        if dictRow['product_id'] in products:
            ids[dictRow['product_id']] += 1
        else:
            products[dictRow['product_id']] = dictRow
            ids[dictRow['product_id']] = 1
    sortedResultIds, res = [], []
    for i, j in sorted(ids.items(), key=lambda item: item[1]):
        sortedResultIds.append({i: j})
    for i in sortedResultIds[-num:]:
        res.append(products[i.keys()[0]])
    return jsonify(res)


@app.route('/product/fraction/<int:number>/<string:startDate>/<string:endDate>/')
def getAllProductsFilteredByFrac(number, startDate, endDate):
    data = session.execute("SELECT product_id, product_title, star_rating, verified_purchase, review_date FROM reviews_by_product WHERE review_date>='{}' AND review_date<='{}' ALLOW FILTERING".format(startDate, endDate))
    products, ids = dict(), dict()
    fiveStarsReview, verifiedYes = '5.0', 'Y'
    lstFiveStart, lstNotFiveStars = [1, 1, 0], [1, 0, 0]
    for i in data:
        dictRow = dict(i)
        dictRow["review_date"] = str(dictRow["review_date"])
        if dictRow['product_id'] in products:
            ids[dictRow['product_id']][0] += 1
            if dictRow['star_rating'] == fiveStarsReview:
                ids[dictRow['product_id']][1] += 1
            
        else:
            products[dictRow['product_id']] = dictRow
            if dictRow['star_rating'] == fiveStarsReview:
                ids[dictRow['product_id']] = lstFiveStart
            else:
                ids[dictRow['product_id']] = lstNotFiveStars
        
        if dictRow['verified_purchase'] == verifiedYes:
            ids[dictRow['product_id']][2] = ids[dictRow['product_id']][2] + 1
        
    sortedResultsIds, res = [], []
    for i, j in sorted(ids.items(), key=lambda item:(item[1][1]/item[1][0]) if item[1][2]>=100 else 0):
        sortedResultsIds.append({i: j})
    
    for i in sortedResultsIds[-number:]:
        res.append(products[i.keys()[0]])
    return jsonify(res)

@app.route('/customer/<int:number>/<string:startDate>/<string:endDate>/')
def getMostProductiveCustomers(number, startDate, endDate):
    data = session.execute("SELECT marketplace, customer_id, vine, verified_purchase, review_date FROM reviews_by_customer WHERE review_date>='{}' AND review_date<='{}' ALLOW FILTERING".format(startDate, endDate))
    customers, ids = dict(), dict()
    verifiedYes, verifiedNo = 'Y', 'N'
    for i in data:
        dictRow = dict(i)
        dictRow["review_date"] = str(dictRow["review_date"])
        if dictRow['customer_id'] in customers:
            if dictRow['verified_purchase'] == verifiedYes:
                ids[dictRow['customer_id']] += 1
        else:
            if dictRow['verified_purchase'] != verifiedNo:
                ids[dictRow['customer_id']] = 1
            customers[dictRow['customer_id']] = dictRow
    
    sortedResultsIds, res = [], []
    for i, j in sorted(ids.items(), key=lambda item: item[1]):
        sortedResultsIds.append({i: j})
    for i in sortedResultsIds[-number:]:
        res.append(products[i.keys()[0]])
    return jsonify(res)


@app.route('/customer/<int:number>/<string:startDate>/<string:endDate>/<string:starRating>/')
def getMostProductiveCustomersWithStar(number, startDate, endDate, starRating):
    data = session.execute("SELECT customer_id, vine, star_rating, review_date FROM reviews_by_customer WHERE review_date>='{}' AND review_date<='{}' ALLOW FILTERING".format(startDate, endDate))
    customers, ids = dict(), dict()
    lowRating, highRating = 'low', 'high'
    for i in data:
        dictRow = dict(i)
        dictRow["review_date"] = str(dictRow["review_date"])
        if dictRow['customer_id'] not in customers:
            customers[dictRow['customer_id']] = dictRow
            ids[dictRow['customer_id']] = 0
        starMatching = (float(dictRow['star_rating']) < 3 and rating == lowRating) or (float(dictRow['star_rating']) > 3 and starRating == highRating)
        if starMatching:
            ids[dictRow['customer_id']] += 1
            
    sortedResultsIds, res = [], []
    for i, j in sorted(ids.items(), key=lambda item: item[1]):
        sortedResultsIds.append({i: j})
    for i in sortedResultsIds[-number:]:
        res.append(products[i.keys()[0]])
    return jsonify(res)


if __name__ == '__main__':
	app.run(host='127.0.0.1', debug=True)
