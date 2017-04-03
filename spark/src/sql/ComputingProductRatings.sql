CREATE VIEW OrdersFullDetails
AS
SELECT orders.id, customerid, productid, categoryid
FROM orders
JOIN customers ON orders.customerid = customers.id
JOIN ordersdetail ON orders.orderdetailid = ordersdetail.id
JOIN products ON ordersdetail.productid = products.id;

CREATE VIEW ProductsRating
AS
SELECT prTemp.customerid, productid,
	 CAST(catTemp.categoryCount * 0.3 + prTemp.productCount * 0.7 AS float) AS rating
FROM (SELECT customerid, COUNT(*) as categoryCount
		FROM OrdersFullDetails
		GROUP BY customerid WITH ROLLUP) catTemp
JOIN (SELECT customerid, productid, COUNT(*) AS productCount
		FROM OrdersFullDetails
		GROUP BY customerid, productid WITH ROLLUP) prTemp
ON catTemp.customerid = prTemp.customerid
WHERE productid IS NOT NULL
