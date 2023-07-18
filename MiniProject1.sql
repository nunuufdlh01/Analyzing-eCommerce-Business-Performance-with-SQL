-- task 1
with info_mau as(
	select 
		date_part('year', ord.order_purchase_timestamp) as year,
		date_part('month', ord.order_purchase_timestamp) as month,
		count(distinct cust.customer_unique_id) as monthly_active_user
	from orders_dataset as ord
	inner join customers_dataset as cust 
		on ord.customer_id = cust.customer_id
	group by year, month
)
select 
	year, 
	round(avg(monthly_active_user), 3) as avg_mau
from info_mau
group by year
order by year
;
--task 2
with info_customers as(
	select 
		min(ord.order_purchase_timestamp) as first_year,
		cust.customer_unique_id as new_customers
	from orders_dataset as ord
	inner join customers_dataset as cust 
		on ord.customer_id = cust.customer_id
	group by new_customers
)
select
	date_part('year', first_year) as year,
	count(new_customers) as new_customer
from info_customers
group by year
order by year
;
--task 3
with info_repeat as(
	select 
		date_part('year', ord.order_purchase_timestamp) as year,
		cust.customer_unique_id as customers,
		count(ord.order_id) as total_order
	from orders_dataset as ord
	inner join customers_dataset as cust 
		on ord.customer_id = cust.customer_id
	group by year, customers
	having count(ord.order_id) > 1
)
select
	year,
	count(customers) as repeat_customer
from info_repeat
group by year
order by year
;
-- task 4
with info_order as(
	select
		date_part('year', ord.order_purchase_timestamp) as year,
		cust.customer_unique_id as customers,
		count(cust.customer_unique_id) as freq_order
	from orders_dataset as ord
	inner join customers_dataset as cust
		on ord.customer_id = cust.customer_id
	group by year, customers
)
select 
	year,
	round(avg(freq_order),3) as avg_freq_order
from info_order
group by year
order by year
;

--task 5 (gabungan)
with info_mau as(
	select 
		year, 
		round(avg(monthly_active_user), 3) as avg_mau
	from(
		select 
			date_part('year', ord.order_purchase_timestamp) as year,
			date_part('month', ord.order_purchase_timestamp) as month,
			count(distinct cust.customer_unique_id) as monthly_active_user
		from orders_dataset as ord
		inner join customers_dataset as cust 
			on ord.customer_id = cust.customer_id
		group by year, month
	) as subq1
	group by year
	order by year
),
info_customers as(
	select 
		date_part('year', first_year) as year,
		count(new_customers) as new_customer
	from(
		select 
			min(ord.order_purchase_timestamp) as first_year,
			cust.customer_unique_id as new_customers
		from orders_dataset as ord
		inner join customers_dataset as cust 
			on ord.customer_id = cust.customer_id
		group by new_customers
	) as subq2
	group by year
	order by year
),
info_repeat as(
	select
		year,
		count(customers) as repeat_customer
	from(
		select 
			date_part('year', ord.order_purchase_timestamp) as year,
			cust.customer_unique_id as customers,
			count(ord.order_id) as total_order
		from orders_dataset as ord
		inner join customers_dataset as cust 
			on ord.customer_id = cust.customer_id
		group by year, customers
		having count(ord.order_id) > 1
	) as subq3
	group by year
	order by year
),
info_order as(
	select
		year,
		round(avg(freq_order),3) as avg_freq_order
	from (
		select
			date_part('year', ord.order_purchase_timestamp) as year,
			cust.customer_unique_id as customers,
			count(cust.customer_unique_id) as freq_order
		from orders_dataset as ord
		inner join customers_dataset as cust
			on ord.customer_id = cust.customer_id
		group by year, customers
	) as subq4
	group by year
	order by year
)
select 
	im.year,
	im.avg_mau,
	ic.new_customer,
	repeat_customer,
	avg_freq_order
from info_mau as im
inner join info_customers as ic
	on im.year = ic.year
inner join info_repeat as ir
	on im.year = ir.year
inner join info_order as io
	on im.year = io.year
;

Tugas 3
--task 1
create table revenue_per_year as
	select
		date_part('year', ord.order_purchase_timestamp) as year,
		sum(oid.price + oid.freight_value) as revenue
	from orders_dataset as ord
	inner join order_items_dataset as oid
		on ord.order_id = oid.order_id
	where ord.order_status = 'delivered'
	group by year
	order by year
;
--task 2
create table cancel_order_per_year as
	select 
		date_part('year', order_purchase_timestamp) as year,
		count(order_id) as cancel_order
	from orders_dataset
	where order_status = 'canceled'
	group by year
	order by year
;
--task 3
create table annual_top_category_by_revenue as
select 
	year,
	top_product,
	revenue
from(
	select 
		date_part('year', ord.order_purchase_timestamp) as year,
		pd.product_category_name as top_product,
		sum(oid.price + oid.freight_value) as revenue,
		rank() over(partition by date_part('year', ord.order_purchase_timestamp)
			order by sum(oid.price + oid.freight_value) desc) as rank
	from orders_dataset as ord
		inner join order_items_dataset as oid
			on ord.order_id = oid.order_id
		inner join product_dataset as pd
			on oid.product_id = pd.product_id
	where ord.order_status = 'delivered'
	group by year, top_product
	order by year
	) as subq1
where rank = 1
;
--task 4
create table annual_top_cancel as
select 
	year,
	top_cancel,
	total_cancel
from(
	select 
		date_part('year', ord.order_purchase_timestamp) as year,
		pd.product_category_name as top_cancel,
		count(ord.order_id) as total_cancel,
		rank() over(partition by date_part('year', ord.order_purchase_timestamp)
				order by count(ord.order_id) desc) as rank
	from orders_dataset as ord
		inner join order_items_dataset as oid
			on ord.order_id = oid.order_id
		inner join product_dataset as pd
			on oid.product_id = pd.product_id
	where ord.order_status = 'canceled'
	group by year, top_cancel
	order by year
	) as subq2
where rank = 1
;
--task 5
select
	ry.year,
	ry.revenue as total_revenue,
	cy.cancel_order as total_cancel,
	ty.top_product,
	ty.revenue as revenue_top_product,
	tc.top_cancel,
	tc.total_cancel as cancel_product
from revenue_per_year as ry
inner join cancel_order_per_year as cy
	on ry.year = cy.year
inner join annual_top_category_by_revenue as ty
	on ry.year = ty.year
inner join annual_top_cancel as tc
	on ry.year = tc.year

Tugas 4
--task 1
select 
	payment_type,
	count(order_id) as total_payment
from order_payments_dataset
group by payment_type
order by total_payment desc
;
--task 2
select
	payment_type,
	sum(case when year = 2016 then 1 else 0 end) as year_2016,
	sum(case when year = 2017 then 1 else 0 end) as year_2017,
	sum(case when year = 2018 then 1 else 0 end) as year_2018
from(
	select
		date_part('year', ord.order_purchase_timestamp) as year,
		pyd.payment_type
	from orders_dataset as ord
	inner join order_payments_dataset as pyd
		on ord.order_id = pyd.order_id
	) as subq1
group by payment_type
order by payment_type
;
--task gabungan (task 1 dan 2)
with info_payment as(
	select 
		payment_type,
		count(order_id) as total_payment
	from order_payments_dataset
	group by payment_type
	order by total_payment desc
),
payment_usage as (
	select
		payment_type,
		sum(case when year = 2016 then 1 else 0 end) as year_2016,
		sum(case when year = 2017 then 1 else 0 end) as year_2017,
		sum(case when year = 2018 then 1 else 0 end) as year_2018
	from(
		select
			date_part('year', ord.order_purchase_timestamp) as year,
			pyd.payment_type
		from orders_dataset as ord
		inner join order_payments_dataset as pyd
			on ord.order_id = pyd.order_id
		) as subq1
	group by payment_type
	order by payment_type
)
select 
	ip.payment_type,
	ip.total_payment,
	pu.year_2016,
	pu.year_2017,
	pu.year_2018
from info_payment as ip
inner join payment_usage as pu
	on ip.payment_type = pu.payment_type
order by ip.total_payment desc
;
