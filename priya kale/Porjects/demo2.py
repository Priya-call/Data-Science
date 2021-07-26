import  mysql.connector

my_conn = mysql.connector.connect(user='devuser', password='password123',
                                host='localhost', database='northwind')

SELECT TOP 10 			 
			c.CompanyName, 
			c.City, 
			c.Country,
			COUNT(o.OrderID) AS CountOrders
FROM		Customers c 
JOIN		Orders o 
ON			c.CustomerID = o.CustomerID
GROUP BY	c.CompanyName, c.City, c.Country
ORDER BY	COUNT(o.OrderId) DESC