delete from staging.user_order_log where date_time::date = '{{ ds }}';

alter table if exists staging.user_order_log
	add column if not exists status varchar(15) NOT NULL Default 'shipped';