create table if not exists mart.f_customer_retention (
    new_customers_count int8,
    returning_customers_count int8,
    refunded_customer_count int8,
    weekly date,
    period_id float8,
    item_id numeric(14,2), 
    new_customers_revenue numeric(14,2), 
    returning_customers_revenue numeric(14,2), 
    customers_refunded numeric(14,2)
);

delete from mart.f_customer_retention where weekly in (select distinct date_trunc('week', '{{ ds }}'::date)::date);

insert into mart.f_customer_retention
with new_customers as (
	select weekly, item_id, count(distinct customer_id) as cust, sum(payment_amount) as payment_amount
	from
		(select customer_id,
				item_id,
				date_trunc('week', date_actual)::date as weekly,
				count(distinct case when payment_amount > 0 then id else null end) as cnt,
				sum(payment_amount) as payment_amount
		from mart.f_sales s
		left join mart.d_calendar dc 
			on s.date_id = dc.date_id 
        where date_trunc('week', date_actual)::date in (select distinct date_trunc('week', '{{ ds }}'::date)::date)
		group by customer_id, item_id, date_trunc('week', date_actual)::date
		having count(distinct case when payment_amount > 0 then id else null end) = 1) c
	group by weekly, item_id),

returning_customers as (
	select weekly, item_id, count(distinct customer_id) as cust, sum(payment_amount) as payment_amount
	from
		(select customer_id,
				item_id,
				date_trunc('week', date_actual)::date as weekly,
				count(distinct case when payment_amount > 0 then id else null end) as cnt,
				sum(payment_amount) as payment_amount
		from mart.f_sales s
		left join mart.d_calendar dc 
			on s.date_id = dc.date_id
        where date_trunc('week', date_actual)::date in (select distinct date_trunc('week', '{{ ds }}'::date)::date)
		group by customer_id, item_id, date_trunc('week', date_actual)::date
		having count(distinct case when payment_amount > 0 then id else null end) > 1) c
	group by weekly, item_id),
	
ref_customers as (
	select weekly, item_id, count(distinct customer_id) as cust, sum(cnt) as refunds
	from
		(select customer_id,
				item_id,
				date_trunc('week', date_actual)::date as weekly,
				count(distinct case when payment_amount < 0 then id else null end) as cnt
		from mart.f_sales s
		left join mart.d_calendar dc 
			on s.date_id = dc.date_id
        where date_trunc('week', date_actual)::date in (select distinct date_trunc('week', '{{ ds }}'::date)::date)
		group by customer_id, item_id, date_trunc('week', date_actual)::date
		) c
	group by weekly, item_id)

select distinct
	nc.cust 							    as new_customers_count,
	rc.cust 							    as returning_customers_count,
	rfc.cust 							    as refunded_customer_count,
	date_trunc('week', dc.date_actual)::date as weekly,
	date_part('month', dc.date_actual)		as period_id,
	s.item_id								as item_id,
	nc.payment_amount                       as new_customers_revenue,
	rc.payment_amount                       as returning_customers_revenue,
	rfc.refunds							    as customers_refunded
from mart.f_sales s
left join mart.d_calendar dc 
	on s.date_id = dc.date_id
left join new_customers nc
	on date_trunc('week', dc.date_actual)::date = nc.weekly and s.item_id = nc.item_id
left join returning_customers rc
	on date_trunc('week', dc.date_actual)::date = rc.weekly and s.item_id = rc.item_id
left join ref_customers rfc
	on date_trunc('week', dc.date_actual)::date = rfc.weekly and s.item_id = rfc.item_id
where date_trunc('week', dc.date_actual)::date in (select distinct date_trunc('week', '{{ ds }}'::date)::date);