drop database if exists cube_test;
create database cube_test;
use cube_test;

-- @bvt:issue#20016
drop table if exists sales_data;
create table sales_data (
year bigint,
quarter smallint,
month int,
region varchar(50),
city varchar(50),
product_category varchar(50),
product_name varchar(100),
sales_rep varchar(100),
amount decimal(10, 2),
quantity double
);

insert into sales_data (year, quarter, month, region, city, product_category, product_name, sales_rep, amount, quantity) values
(2021, 1, 1, 'North', 'New York', 'Electronics', 'Laptop', 'Alice', 1500.00, 2),
(2021, 1, 1, 'North', 'New York', 'Electronics', 'Smartphone', 'Alice', 1000.00, 3),
(2021, 1, 2, 'South', 'Los Angeles', 'Books', 'Book A', 'Bob', 300.00, 5),
(2021, 1, 2, 'South', 'Los Angeles', 'Books', 'Book B', 'Bob', 200.00, 4),
(2021, 2, 3, 'East', 'Chicago', 'Clothing', 'T-Shirt', 'charlie', 500.00, 10),
(2021, 2, 3, 'East', 'Chicago', 'Clothing', 'Jeans', 'charlie', 800.00, 6),
(2021, 2, 4, 'West', 'San Francisco', 'Electronics', 'Laptop', 'David', 1800.00, 3),
(2021, 2, 4, 'West', 'San Francisco', 'Electronics', 'Smartphone', 'David', 1200.00, 5),
(2022, 1, 1, 'North', 'New York', 'Electronics', 'Laptop', 'Alice', 1600.00, 2),
(2022, 1, 1, 'North', 'New York', 'Electronics', 'Smartphone', 'Alice', 1100.00, 3);

select
    if(grouping(year), 'Total', cast(year as char)) as year,
    if(grouping(quarter), 'Total', cast(quarter as char)) as quarter,
    if(grouping(region), 'Total', region) as region,
    if(grouping(city), 'Total', city) as city,
    sum(amount) as total_sales,
    sum(quantity) as total_quantity
from
    sales_data
group by
    cube(year, quarter, region, city)
order by
    year is null,
    year,
    quarter is null,
    quarter,
    region is null,
    region,
    city is null,
    city;

select
    year,
    quarter,
    region,
    city,
    avg(amount) as total_sales,
    avg(quantity) as total_quantity
from
    sales_data
group by
    cube(year, quarter, region, city)
order by
    year is null,
    year,
    quarter is null,
    quarter,
    region is null,
    region,
    city is null,
    city;

select
    year,
    quarter,
    region,
    city,
    avg(amount) as total_sales,
    avg(quantity) as total_quantity
from
    sales_data
group by
    cube(year, quarter, region, city)
order by
    year is null,
    year,
    quarter is null,
    quarter,
    region is null,
    region,
    city is null,
    city;

drop table sales_data;
-- @bvt:issue
drop database cube_test;