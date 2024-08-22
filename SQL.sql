/* Write a Query to Print the position of the character ‘a’ in the First Names and Last Names of
Customers. */

select position('a' in FirstName) as position_a_FirstName, position('a' in LastName) as position_a_LastName
from customers_ecom

/* Write a Query to Print the Position of the substring ‘ch’ in the First Names of all the
Customers. */

select position('ch' in FirstName) as position_ch_FirstName from customers_ecom
 
/* Write a Query to print the Position of the character ‘e’ in the First Names of all customers,
if ‘e’ does not exist in the first name print NULL instead of 0. */

select case when position('e' in FirstName) = 0 then NULL 
	        else position('e' in FirstName) end as position_e
from customers_ecom

/* Write a Query to print the time taken in days to deliver orders from the shipping date for each
order in the Orders table. */

select extract(DOY from delivarydate) - extract(DOY from shipdate) as deliver_time from orders_ecom

/* Write a query to print all customer details. Order your output in ascending order of the first 3
characters of FirstName. */

select * from customers_ecom 
order by left(firstname, 3)

/* Write a query to print pairs of customers who belong to the same state. */

select 
    c1.FirstName AS Customer1_FirstName,
    c1.LastName AS Customer1_LastName,
    c2.FirstName AS Customer2_FirstName,
    c2.LastName AS Customer2_LastName,
    c1.State AS Shared_State
from 
    customers_ecom c1,
    customers_ecom c2
where 
    c1.State = c2.State 
    AND c1.custermorid < c2.custermorid
order by  
    c1.State, c1.FirstName, c2.FirstName

/* Get the total number of records in the table. */

select count(*) as tot_records from customers_ecom

/* Get the Total Number of Records for Brand Harpic. */

select count(productid) as tot_records_harpic from products_ecom 
       where brand = 'Harpic'

/* Get the total Number of DISTINCT records for the Brand Harpic. What is the difference from
the output of the above query - and why? */

select count(distinct productid) as distinct_records_harpic from products_ecom 
       where brand = 'Harpic'

/* Get the Total Number of Records for Brand Lizol. */

select count(productid) as tot_records_lizol from products_ecom 
       where brand = 'Lizol'

/* Get the total number of records for each Brand in a single output. */

select count(productid), brand from products_ecom
        group by brand
        order by count(productid)

/* Calculate the AVERAGE market Price for all products belonging to Type "Nachos & Chips". */

select round(avg(market_price), 2) from products_ecom
       where type = 'Nachos & Chips'

/* Get the SUM of Market Price of all Products belonging to Type "Dry Fruits & Berries". */ 

select round(sum(market_price), 2) from products_ecom
       where type = 'Dry Fruits & Berries'

/* Get the MAX Sale Price of Products across each Sub-Category. */

select max(sale_price), sub_category from products_ecom
       group by sub_category
       order by max(sale_price)

/* Get the DISTINCT Count of Categories. */

select count(distinct categoryname) from category_ecom

/* Get the DISTINCT Count of Products of the Type "Canned Seafood". */

select count(distinct product) from products_ecom 
       where type = 'Canned Seafood'

/* Identify the count of distinct products that the company sells within each category. */

select count(distinct product), category_id from products_ecom 
       group by category_id

/* Identify the average order amount by each CustomerID in each month of Year 2020. */

select customerid, extract(month from orderdate) as months,
       cast(avg(total_order_amount) as numeric) as avg_order_amount
from orders_ecom
where extract(year from orderdate) = 2020
group by months, customerid

/* Identify the Month-Year combinations which had the highest customer acquisition. */

select extract(month from dateentered) || '-' || extract(year from dateentered) as month_year, 
	count(distinct custermorid) as customer_acquisition
from customers_ecom
group by month_year
order by customer_acquisition desc
limit 1

/* Identify the most selling ProductID in 2021. */

select productid, 
       count(*) as total_sales
from products_ecom, orders_ecom
where extract(year from orderdate) = 2021
group by ProductiD
order by total_sales desc
limit 1;

/* Identify which Supplier ID supplied the least number of products. */

select supplierid, count(*) as tot_supplies from suppliers_ecom
group by supplierid
order by tot_supplies 
limit 1

/* Get details of those customers who have ordered for a total amount of more than 7000 during
last quarter of Year 21. */

select customerid, cast(sum(total_order_amount) as numeric) from orders_ecom 
	where orderdate between '2021-10-01' AND '2021-12-31'
group by customerid
having sum(total_order_amount) > 7000 

/* Find the no. of orders fulfilled by Suppliers residing in the same Country as the customer. */

select count(distinct o.OrderID) AS num_orders
FROM orders_ecom o
JOIN customers_ecom ce ON o.CustomerID = ce.custermorid
JOIN orderdetails_ecom od ON o.OrderID = od.OrderID
JOIN suppliers_ecom s ON od.SupplierID = s.SupplierID
where ce.Country = s.Country


/* Find out the top 4 best-selling products in each of the categories that are currently active on
the Website */

SELECT 
        p.ProductID,
        p.Category_ID,
        SUM(od.Quantity) AS total_sales
    FROM 
        products_ecom p
    JOIN 
        orderdetails_ecom od ON p.ProductID = od.ProductID
    JOIN 
        category_ecom c ON p.Category_ID = c.Category_ID
    WHERE 
        c.active = 'Yes'
    GROUP BY 
        p.ProductID, p.Category_ID
    ORDER BY 
        total_sales desc
    LIMIT 4

/* Find the out the least selling products in each of the categories that are currently active on
the website. */

SELECT 
        p.ProductID,
        p.Category_ID,
        SUM(od.Quantity) AS total_sales
    FROM 
        products_ecom p
    JOIN 
        orderdetails_ecom od ON p.ProductID = od.ProductID
    JOIN 
        category_ecom c ON p.Category_ID = c.Category_ID
    WHERE 
        c.active = 'Yes'
    GROUP BY 
        p.ProductID, p.Category_ID
    ORDER BY 
        total_sales 
    LIMIT 4

/* Find the cumulative sum of total orders placed for the year 2020. */

select orderdate, count(orderid), sum(count(orderid)) over (order by orderdate)
from orders_ecom 
where extract(year from orderdate) = 2020
group by orderdate
order by orderdate

/* ) Find the top 25 customers in terms of
a) Total no. of orders placed for Year 2021
b) Total Purchase Amount for the Year 2021 */

select 
    o.CustomerID,
    COUNT(o.OrderID) AS total_orders
FROM 
    orders_ecom o
WHERE 
    EXTRACT(YEAR FROM o.OrderDate) = 2021
GROUP BY 
    o.CustomerID
ORDER BY 
    total_orders DESC
LIMIT 25

select 
    o.CustomerID,
    round(cast(sum(o.total_order_amount) as numeric), 2) as total_orders_amount
FROM 
    orders_ecom o
WHERE 
    EXTRACT(YEAR FROM o.OrderDate) = 2021
GROUP BY 
    o.CustomerID
ORDER BY 
    total_orders_amount DESC
LIMIT 25

/* ) Find the cumulative average order amount at a monthly level for year 2021
a) Each category
b) Each customer */

select customerid, extract(month from orderdate) as month_order,
       avg(total_order_amount) over (partition by customerid order by extract(month from orderdate))
from orders_ecom
where extract(year from orderdate) = 2021
order by customerid, month_order

SELECT c.CategoryName, EXTRACT(MONTH FROM o.OrderDate) AS order_month,
    AVG(SUM(o.total_order_amount)) OVER (PARTITION BY c.Category_ID ORDER BY EXTRACT(MONTH FROM o.OrderDate)) AS cumulative_avg_order_amount
FROM orders_ecom o
JOIN orderdetails_ecom od ON o.OrderID = od.OrderID
JOIN products_ecom p ON od.ProductID = p.ProductID
JOIN category_ecom c ON p.Category_ID = c.Category_ID
WHERE EXTRACT(YEAR FROM o.OrderDate) = 2021
GROUP BY c.Category_ID, EXTRACT(MONTH FROM o.OrderDate), c.CategoryName
ORDER BY c.CategoryName, order_month

/* Find the 3-day rolling average for the total purchase amount by each customer. */

select customerid, orderdate, avg(sum(total_order_amount)) over (partition by customerid order by orderdate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
from orders_ecom 
group by customerid, orderdate
order by customerid, orderdate

/* Get the cumulative sum of total_order_amount for orders placed by each customer ordered
by the orderID. */

SELECT CustomerID,OrderID, 
    SUM(total_order_amount) OVER (PARTITION BY CustomerID ORDER BY OrderID) AS cumulative_sum
FROM orders_ecom
ORDER BY CustomerID, OrderID

/* Print the cumulative sum of Total_Transaction_Value for each of the months of the year
2020. */

SELECT EXTRACT(MONTH FROM OrderDate) AS month, SUM(total_order_amount) AS monthly_total,
      SUM(SUM(total_order_amount)) OVER (ORDER BY EXTRACT(MONTH FROM OrderDate)) AS cumulative_sum
FROM orders_ecom
WHERE EXTRACT(YEAR FROM OrderDate) = 2020
GROUP BY EXTRACT(MONTH FROM OrderDate)
ORDER BY month

/* Print the cumulative average of the quantity of products ordered in each quarter of the years
2020 and 2021. */

WITH QuarterlyProductQuantities AS (
    SELECT
        EXTRACT(YEAR FROM o.OrderDate) AS year,
        EXTRACT(QUARTER FROM o.OrderDate) AS quarter,
        SUM(od.Quantity) AS total_quantity
    FROM 
        orders_ecom o
    JOIN 
        orderdetails_ecom od ON o.OrderID = od.OrderID
    WHERE 
        EXTRACT(YEAR FROM o.OrderDate) IN (2020, 2021)
    GROUP BY 
        EXTRACT(YEAR FROM o.OrderDate),
        EXTRACT(QUARTER FROM o.OrderDate)
)
SELECT
    year,
    quarter,
    cast(total_quantity as numeric),
    AVG(total_quantity) OVER (
        PARTITION BY year
        ORDER BY quarter
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_avg_quantity 
FROM 
    QuarterlyProductQuantities
ORDER BY 
    year, quarter

/* Identify and print the details of products that were the second most ordered in terms of total
quantity for each month of each year */

WITH ProductRanks AS (
    SELECT
        EXTRACT(YEAR FROM o.OrderDate) AS year,
        EXTRACT(MONTH FROM o.OrderDate) AS month,
        od.ProductID,
        SUM(od.Quantity) AS total_quantity,
        RANK() OVER (
            PARTITION BY EXTRACT(YEAR FROM o.OrderDate), EXTRACT(MONTH FROM o.OrderDate)
            ORDER BY SUM(od.Quantity) DESC
        ) AS rank
    FROM 
        orders_ecom o
    JOIN 
        orderdetails_ecom od ON o.OrderID = od.OrderID
    GROUP BY 
        EXTRACT(YEAR FROM o.OrderDate),
        EXTRACT(MONTH FROM o.OrderDate),
        od.ProductID
)
SELECT
    year,
    month,
    ProductID,
    total_quantity
FROM 
    ProductRanks
WHERE 
    rank = 2
ORDER BY 
    year, month

/* Identify and print the details of products that were the that generated the 5th most revenue
for each quarter of each year. */

WITH RevenueByQuarter AS (
    SELECT
        EXTRACT(YEAR FROM o.OrderDate) AS year,
        EXTRACT(QUARTER FROM o.OrderDate) AS quarter,
        od.ProductID,
        SUM(od.Quantity * o.total_order_amount) AS total_revenue
    FROM 
        orders_ecom o
    JOIN 
        orderdetails_ecom od ON o.OrderID = od.OrderID
    GROUP BY 
        year, quarter, od.ProductID
)
SELECT
    year,
    quarter,
    ProductID,
    total_revenue
FROM (
    SELECT
        year,
        quarter,
        ProductID,
        cast(total_revenue as numeric),
        RANK() OVER (PARTITION BY year, quarter ORDER BY total_revenue DESC) AS rank
    FROM 
        RevenueByQuarter
) Ranked
WHERE 
    rank = 5
ORDER BY 
    year, quarter

/* Print the details of total transactions value made on each day whose info is available in the
database. */

SELECT o.OrderDate AS transaction_date,
    SUM(o.total_order_amount) AS total_transaction_value
FROM orders_ecom o
GROUP BY o.OrderDate
ORDER BY o.OrderDate

/* Along with the output of the above question, print a new column which provides a 5 day
rolling average of the total transaction value made each day. */

WITH DailyTotals AS (
    SELECT
        OrderDate AS transaction_date,
        SUM(total_order_amount) AS total_transaction_value
    FROM
        orders_ecom
    GROUP BY
        OrderDate
)
SELECT
    transaction_date,
    total_transaction_value,
    AVG(total_transaction_value) OVER (
        ORDER BY transaction_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5_day_avg
FROM
    DailyTotals
ORDER BY
    transaction_date

/* Print the year, month, productID, Product_Name, Total_Quantity, Total_Revenue for those
products that were ordered the most number of times in terms of total_quantity for each year
and quarter combination. */

WITH ProductStats AS (
    SELECT
        EXTRACT(YEAR FROM o.OrderDate) AS year,
        EXTRACT(QUARTER FROM o.OrderDate) AS quarter,
        od.ProductID,
        p.Product,
        SUM(od.Quantity) AS Total_Quantity,
        SUM(od.Quantity * o.total_order_amount) AS Total_Revenue
    FROM
        orders_ecom o
    JOIN
        orderdetails_ecom od ON o.OrderID = od.OrderID
    JOIN
        products_ecom p ON od.ProductID = p.ProductID
    GROUP BY
        EXTRACT(YEAR FROM o.OrderDate),
        EXTRACT(QUARTER FROM o.OrderDate),
        od.ProductID,
        p.Product
),
RankedProducts AS (
    SELECT
        year,
        quarter,
        ProductID,
        Product,
        Total_Quantity,
        Total_Revenue,
        RANK() OVER (
            PARTITION BY year, quarter
            ORDER BY Total_Quantity DESC
        ) AS rank
    FROM
        ProductStats
)
SELECT
    year,
    quarter,
    ProductID,
    Product,
    Total_Quantity,
    Total_Revenue
FROM
    RankedProducts
WHERE
    rank = 1
ORDER BY
    year, quarter

/* Using the result set of the above question, compare and find the maximum Total_Quantity
for each row, comparing each row values with 2 Previous months and 1 following month. */

WITH ProductStats AS (
    SELECT
        EXTRACT(YEAR FROM o.OrderDate) AS year,
        EXTRACT(MONTH FROM o.OrderDate) AS month,
        od.ProductID,
        p.Product AS product,
        SUM(od.Quantity) AS Total_Quantity
    FROM
        orders_ecom o
    JOIN
        orderdetails_ecom od ON o.OrderID = od.OrderID
    JOIN
        products_ecom p ON od.ProductID = p.ProductID
    GROUP BY
        year, month, od.ProductID, p.Product
),
RankedProducts AS (
    SELECT
        year,
        month,
        ProductID,
        product,
        Total_Quantity,
        RANK() OVER (PARTITION BY year, month ORDER BY Total_Quantity DESC) AS rank
    FROM
        ProductStats
)
SELECT
    year,
    month,
    ProductID,
    product,
    Total_Quantity,
    GREATEST(
        Total_Quantity,
        LAG(Total_Quantity, 2) OVER (PARTITION BY ProductID ORDER BY year, month),
        LAG(Total_Quantity, 1) OVER (PARTITION BY ProductID ORDER BY year, month),
        LEAD(Total_Quantity, 1) OVER (PARTITION BY ProductID ORDER BY year, month)
    ) AS max_quantity_comparison
FROM
    RankedProducts
WHERE
    rank = 1

/* Print the Year, Month, PaymentID, PaymentType, Total_Transaction_Value for those
PaymentTypes that had the highest transaction values for for each year and month combination. */

WITH TransactionStats AS (
    SELECT
        EXTRACT(YEAR FROM o.OrderDate) AS year,
        EXTRACT(MONTH FROM o.OrderDate) AS month,
        o.PaymentID,
        p.PaymentType,
        SUM(o.total_order_amount) AS Total_Transaction_Value
    FROM
        orders_ecom o
    JOIN
        payments_ecom p ON o.PaymentID = p.PaymentID
    GROUP BY
        year, month, o.PaymentID, p.PaymentType
),
RankedPayments AS (
    SELECT
        year,
        month,
        PaymentID,
        PaymentType,
        Total_Transaction_Value,
        RANK() OVER (PARTITION BY year, month ORDER BY Total_Transaction_Value DESC) AS rank
    FROM
        TransactionStats
)
SELECT
    year,
    month,
    PaymentID,
    PaymentType,
    Total_Transaction_Value
FROM
    RankedPayments
WHERE
    rank = 1
ORDER BY
    year, month

/* Using the result set of the above question, calculate the average total_transaction_value for
the previous month, current month and 1 following month. */

WITH TransactionStats AS (
    SELECT
        EXTRACT(YEAR FROM o.OrderDate) AS year,
        EXTRACT(MONTH FROM o.OrderDate) AS month,
        o.PaymentID,
        p.PaymentType,
        SUM(o.total_order_amount) AS Total_Transaction_Value
    FROM
        orders_ecom o
    JOIN
        payments_ecom p ON o.PaymentID = p.PaymentID
    GROUP BY
        year, month, o.PaymentID, p.PaymentType
),
RankedPayments AS (
    SELECT
        year,
        month,
        PaymentID,
        PaymentType,
        Total_Transaction_Value,
        RANK() OVER (PARTITION BY year, month ORDER BY Total_Transaction_Value DESC) AS rank
    FROM
        TransactionStats
),
RankedPaymentsWithLagLead AS (
    SELECT
        year,
        month,
        PaymentID,
        PaymentType,
        Total_Transaction_Value,
        LAG(Total_Transaction_Value, 1) OVER (PARTITION BY PaymentID ORDER BY year, month) AS prev_month_value,
        LEAD(Total_Transaction_Value, 1) OVER (PARTITION BY PaymentID ORDER BY year, month) AS next_month_value
    FROM
        RankedPayments
    WHERE
        rank = 1
)
SELECT
    year,
    month,
    PaymentID,
    PaymentType,
    Total_Transaction_Value,
    (Total_Transaction_Value +
     COALESCE(prev_month_value, 0) +
     COALESCE(next_month_value, 0)) / 
     (CASE 
         WHEN prev_month_value IS NOT NULL AND next_month_value IS NOT NULL THEN 3
         WHEN prev_month_value IS NULL AND next_month_value IS NOT NULL THEN 2
         WHEN prev_month_value IS NOT NULL AND next_month_value IS NULL THEN 2
         ELSE 1
     END) AS avg_transaction_value
FROM
    RankedPaymentsWithLagLead
ORDER BY
    year, month

/* Print the details of the immediately previous order along with the current order details from
the orders table. */

WITH OrderDetails AS (
    SELECT
        OrderID,
        CustomerID,
        PaymentID,
        OrderDate,
        total_order_amount,
        LAG(OrderID) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS Prev_OrderID,
        LAG(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS Prev_OrderDate,
        LAG(total_order_amount) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS Prev_Total_Order_Amount
    FROM
        orders_ecom
)
SELECT
    OrderID AS Current_OrderID,
    CustomerID,
    PaymentID,
    OrderDate AS Current_OrderDate,
    total_order_amount AS Current_Total_Order_Amount,
    Prev_OrderID,
    Prev_OrderDate,
    Prev_Total_Order_Amount
FROM
    OrderDetails
ORDER BY
    OrderDate

/*  Print CustomerID, FirstName, LastName, OrderId, OrderDate, Previous_Order_Id,
Previous_Order_date. */

WITH OrderDetails AS (
    SELECT
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        LAG(o.OrderID) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS Previous_Order_ID,
        LAG(o.OrderDate) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS Previous_Order_Date
    FROM
        orders_ecom o
),
CustomerOrderDetails AS (
    SELECT
        ce.CustermorID,
        ce.FirstName,
        ce.LastName,
        o.OrderID,
        o.OrderDate,
        o.Previous_Order_ID,
        o.Previous_Order_Date
    FROM
        OrderDetails o
    JOIN
        customers_ecom ce ON o.CustomerID = ce.CustermorID
)
SELECT
    CustermorID,
    FirstName,
    LastName,
    OrderID,
    OrderDate,
    Previous_Order_ID,
    Previous_Order_Date
FROM
    CustomerOrderDetails
ORDER BY
    CustermorID, OrderDate

/* Identify the top 20 products sold in terms of total revenue. */

SELECT
    p.ProductID,
    p.Product,
    SUM(od.Quantity * p.Sale_Price) AS Total_Revenue
FROM
    orderdetails_ecom od
JOIN
    products_ecom p ON od.ProductID = p.ProductID
GROUP BY
    p.ProductID, p.Product
ORDER BY
    Total_Revenue DESC
LIMIT 20

/* Identify the top 12 products in terms of Total Quantity. */

SELECT
    p.ProductID,
    p.Product,
    SUM(od.Quantity) AS Total_Quantity
FROM
    orderdetails_ecom od
JOIN
    products_ecom p ON od.ProductID = p.ProductID
GROUP BY
    p.ProductID, p.Product
ORDER BY
    Total_Quantity DESC
LIMIT 12

/* Create a Year on Year Analysis in which you print the total transaction amount for each
quarter of 2020, then print the total transaction amount for each quarter of 2021 in 2 new columns
(Columns to be printed: Year, Quarter, Total Transaction Amount, Next_Year,
Next_Year_Quarter, Next_Year_Total_Transaction_Amount). */

WITH QuarterlyTransaction AS (
    SELECT
        EXTRACT(YEAR FROM OrderDate) AS Year,
        EXTRACT(QUARTER FROM OrderDate) AS Quarter,
        SUM(total_order_amount) AS Total_Transaction_Amount
    FROM
        orders_ecom
    WHERE
        EXTRACT(YEAR FROM OrderDate) IN (2020, 2021)
    GROUP BY
        Year, Quarter
)
SELECT
    t2020.Year,
    t2020.Quarter,
    t2020.Total_Transaction_Amount,
    2021 AS Next_Year,
    t2021.Quarter AS Next_Year_Quarter,
    COALESCE(t2021.Total_Transaction_Amount, 0) AS Next_Year_Total_Transaction_Amount
FROM
    QuarterlyTransaction t2020
LEFT JOIN
    QuarterlyTransaction t2021
ON
    t2020.Quarter = t2021.Quarter AND t2021.Year = 2021
WHERE
    t2020.Year = 2020
ORDER BY
    t2020.Quarter

/* Identify the number of orders spent by each customer, then divide them into 3 buckets, give
the 3 buckets the tags: Shopping Freak for bucket-1, Regular Customer for bucket-2,
Occasional Customer for bucket-3. */

WITH CustomerOrderCounts AS (
    SELECT
        CustomerID,
        COUNT(OrderID) AS OrderCount
    FROM
        orders_ecom
    GROUP BY
        CustomerID
),
RankedCustomers AS (
    SELECT
        CustomerID,
        OrderCount,
        ROW_NUMBER() OVER (ORDER BY OrderCount DESC) AS RowNum,
        COUNT(*) OVER () AS TotalCount
    FROM
        CustomerOrderCounts
)
SELECT
    CustomerID,
    OrderCount,
    CASE
        WHEN RowNum <= TotalCount / 3 THEN 'Shopping Freak'
        WHEN RowNum <= 2 * TotalCount / 3 THEN 'Regular Customer'
        ELSE 'Occasional Customer'
    END AS CustomerType
FROM
    RankedCustomers
ORDER BY
    OrderCount DESC

/* Find out the least selling products in each of the categories (in the Categories that are
currently active on the website).*/

WITH ActiveCategories AS (
    SELECT
        category_id
    FROM
        category_ecom
    WHERE
        active = 'Yes'
),
ProductSales AS (
    SELECT
        p.productid,
        p.category_id AS categoryid,
        SUM(od.quantity) AS total_quantity
    FROM
        orderdetails_ecom od
    JOIN
        products_ecom p ON od.productid = p.productid
    WHERE
        p.category_id IN (SELECT category_id FROM ActiveCategories)
    GROUP BY
        p.productid, p.category_id
),
LeastSellingProducts AS (
    SELECT
        ps.categoryid,
        ps.productid,
        ps.total_quantity,
        RANK() OVER (PARTITION BY ps.categoryid ORDER BY ps.total_quantity ASC) AS rank
    FROM
        ProductSales ps
)
SELECT
    ac.categoryname,
    p.productid,
    p.product, 
    lsp.total_quantity
FROM
    LeastSellingProducts lsp
JOIN
    category_ecom ac ON lsp.categoryid = ac.category_id
JOIN
    products_ecom p ON lsp.productid = p.productid
WHERE
    lsp.rank = 1
ORDER BY
    ac.categoryname, lsp.total_quantity

/* Print the details of the 10th most ordered products in each month of each year. */

WITH RankedProducts AS (
    SELECT
        EXTRACT(YEAR FROM o.orderdate) AS year,
        EXTRACT(MONTH FROM o.orderdate) AS month,
        p.productid,
        p.product,
        SUM(od.quantity) AS total_quantity,
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM o.orderdate), EXTRACT(MONTH FROM o.orderdate) ORDER BY SUM(od.quantity) DESC) AS rank
    FROM
        orders_ecom o
    JOIN
        orderdetails_ecom od ON o.orderid = od.orderid
    JOIN
        products_ecom p ON od.productid = p.productid
    GROUP BY
        year, month, p.productid, p.product
)
SELECT
    year,
    month,
    productid,
    product,
    total_quantity
FROM
    RankedProducts
WHERE
    rank = 10
ORDER BY
    year, month

/* Rank the customers based on the date on which their details were entered. The oldest
entered customer will get rank 1 and so on. */

SELECT
    CustermorID,
    FirstName,
    LastName,
    dateentered, 
    RANK() OVER (ORDER BY dateentered ASC) AS customer_rank
FROM
    customers_ecom
ORDER BY
    customer_rank

/* Rank the customers on the basis of their ages. Give the oldest customer the rank 1. */

SELECT
    CustermorID,
    FirstName,
    LastName,
    date_of_birth, 
    RANK() OVER (ORDER BY date_of_birth ASC) AS customer_rank
FROM
    customers_ecom
ORDER BY
    customer_rank

/* Get details of the customers whose details were entered very first amongst their respective
countries. */

WITH FirstCustomerByCountry AS (
    SELECT
        country,
        MIN(dateentered) AS first_entry_date
    FROM
        customers_ecom
    GROUP BY
        country
)
SELECT
    c.CustermorID,
    c.FirstName,
    c.LastName,
    c.Country,
    c.dateentered
FROM
    customers_ecom c
JOIN
    FirstCustomerByCountry fc
ON
    c.country = fc.country AND c.dateentered = fc.first_entry_date

/* Get the most ordered product’s details in each quarter of each year. */

WITH QuarterlyProductOrders AS (
    SELECT
        p.ProductID,
        p.Product,
        EXTRACT(YEAR FROM o.OrderDate) AS Year,
        EXTRACT(QUARTER FROM o.OrderDate) AS Quarter,
        SUM(od.Quantity) AS TotalQuantity,
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM o.OrderDate), EXTRACT(QUARTER FROM o.OrderDate) 
                     ORDER BY SUM(od.Quantity) DESC) AS rnk
    FROM
        orders_ecom o
    JOIN
        orderdetails_ecom od ON o.OrderID = od.OrderID
    JOIN
        products_ecom p ON od.ProductID = p.ProductID
    GROUP BY
        p.ProductID, p.Product, EXTRACT(YEAR FROM o.OrderDate), EXTRACT(QUARTER FROM o.OrderDate)
)
SELECT
    ProductID,
    Product,
    Year,
    Quarter,
    TotalQuantity
FROM
    QuarterlyProductOrders
WHERE
    rnk = 1

/* Get the details of category whose products where ordered the most in each month of each
year. */

WITH MonthlyCategoryOrders AS (
    SELECT
        c.category_id,
        c.categoryname,
        EXTRACT(YEAR FROM o.OrderDate) AS Year,
        EXTRACT(MONTH FROM o.OrderDate) AS Month,
        SUM(od.Quantity) AS TotalQuantity,
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM o.OrderDate), EXTRACT(MONTH FROM o.OrderDate)
                     ORDER BY SUM(od.Quantity) DESC) AS rnk
    FROM
        orders_ecom o
    JOIN
        orderdetails_ecom od ON o.OrderID = od.OrderID
    JOIN
        products_ecom p ON od.ProductID = p.ProductID
    JOIN
        category_ecom c ON p.category_id = c.category_id
    GROUP BY
        c.category_id, c.categoryname, EXTRACT(YEAR FROM o.OrderDate), EXTRACT(MONTH FROM o.OrderDate)
)
SELECT
    category_id,
    categoryname,
    Year,
    Month,
    TotalQuantity
FROM
    MonthlyCategoryOrders
WHERE
    rnk = 1

/* Get the details of the orders placed by customers placed by them the 5th time. */

WITH CustomerOrderRank AS (
    SELECT
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        o.Total_Order_Amount,
        ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS order_rank
    FROM
        orders_ecom o
)
SELECT
    OrderID,
    CustomerID,
    OrderDate,
    Total_Order_Amount
FROM
    CustomerOrderRank
WHERE
    order_rank = 5

/* Identify the age of top 10 customers who spent the most. */

SELECT
    c.CustermorID,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.date_of_birth)) AS Age,
    SUM(o.Total_Order_Amount) AS TotalSpending
FROM
    customers_ecom c
JOIN
    orders_ecom o ON c.CustermorID = o.CustomerID
GROUP BY
    c.CustermorID, c.date_of_birth
ORDER BY
    TotalSpending DESC
LIMIT 10

/* Identify the count of brands whose products the company sells within each category. */

SELECT
    p.category_id,
    COUNT(DISTINCT p.brand) AS brand_count
FROM
    products_ecom p
GROUP BY
    p.category_id

/* Print details of customers along with details of total orders and total spend. For those
customers who ordered more than thrice and received those orders in less than 7 days. */

WITH OrderSummary AS (
    SELECT
        o.CustomerID,
        COUNT(o.OrderID) AS order_count,
        SUM(o.Total_Order_Amount) AS total_spend
    FROM
        orders_ecom o
    WHERE
        extract(DOY from o.delivarydate) - extract(DOY from o.orderdate) < 7
    GROUP BY
        o.CustomerID
)
SELECT
    c.CustermorID,
    c.FirstName,
    c.LastName,
    os.order_count AS TotalOrders,
    os.total_spend AS TotalSpend
FROM
    customers_ecom c
JOIN
    OrderSummary os ON c.CustermorID = os.CustomerID
WHERE
    os.order_count > 3

/* Print details of the 5 most ordered products. */

SELECT
    p.ProductID,
    p.Product,  
    SUM(od.Quantity) AS TotalQuantity
FROM
    products_ecom p
JOIN
    orderdetails_ecom od ON p.ProductID = od.ProductID
GROUP BY
    p.ProductID, p.Product  
ORDER BY
    TotalQuantity DESC
LIMIT 5

/* Print the details of the payment method from each quarter of each year which had the
highest transaction value. */

WITH QuarterlyTransaction AS (
    SELECT
        EXTRACT(YEAR FROM o.OrderDate) AS Year,
        EXTRACT(QUARTER FROM o.OrderDate) AS Quarter,
        p.PaymentID,
        p.PaymentType,
        SUM(o.Total_Order_Amount) AS TotalTransactionValue
    FROM
        orders_ecom o
    JOIN
        payments_ecom p ON o.PaymentID = p.PaymentID
    GROUP BY
        Year, Quarter, p.PaymentID, p.PaymentType
),
RankedTransactions AS (
    SELECT
        Year,
        Quarter,
        PaymentID,
        PaymentType,
        TotalTransactionValue,
        RANK() OVER (PARTITION BY Year, Quarter ORDER BY TotalTransactionValue DESC) AS rnk
    FROM
        QuarterlyTransaction
)
SELECT
    Year,
    Quarter,
    PaymentID,
    PaymentType,
    TotalTransactionValue
FROM
    RankedTransactions
WHERE
    rnk = 1


























