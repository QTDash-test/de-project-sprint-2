select distinct shipping_transfer_description
from public.shipping

--- public.shipping_country_rates
create table if not exists public.shipping_country_rates
	(shipping_country_id 		serial,
	shipping_country			text,
	shipping_country_base_rate 	numeric(14,3),
	constraint shipping_country_id_pkey primary key (shipping_country_id));

insert into public.shipping_country_rates (shipping_country, shipping_country_base_rate)
	select distinct shipping_country, shipping_country_base_rate
	from public.shipping;
--- select * from public.shipping_country_rates limit 10


--- public.shipping_agreement
create table if not exists public.shipping_agreement
	(agreementid 				integer,
	agreement_number			text,
	agreement_rate				numeric(14,3),
	agreement_commission		numeric(14,3),
	constraint agreementid_id_pkey primary key (agreementid));

insert into public.shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
	select distinct
		description[1]::integer 		as agreementid,
		description[2]::text 			as agreement_number,
		description[3]::numeric(14,3) 	as agreement_rate,
		description[4]::numeric(14,3) 	as agreement_commission
	from
		(select regexp_split_to_array(vendor_agreement_description, ':+') as description
		from public.shipping)t;
--- select * from public.shipping_agreement limit 10


--- public.shipping_transfer
create table if not exists public.shipping_transfer
	(transfer_type_id 			serial,
	transfer_type				text,
	transfer_model				text,
	shipping_transfer_rate		numeric(14,3),
	constraint transfer_type_id_pkey primary key (transfer_type_id));

insert into public.shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
	select distinct
		description[1]::text 			as transfer_type,
		description[2]::text 			as transfer_model,
		shipping_transfer_rate			as shipping_transfer_rate
	from
		(select regexp_split_to_array(shipping_transfer_description, ':+') as description,
				shipping_transfer_rate
		from public.shipping)t;
--- select * from public.shipping_transfer limit 10


--- public.shipping_info
create table if not exists public.shipping_info
	(shippingid		 			int8,
	vendorid					int8,
	payment_amount				numeric(14,2),
	shipping_plan_datetime	timestamp,
	transfer_type_id			int,
	shipping_country_id			int,
	agreementid					int,
	constraint shippingid_id_pkey primary key (shippingid),
	constraint transfer_type_id_fkey foreign key (transfer_type_id) references public.shipping_transfer(transfer_type_id),
	constraint shipping_country_id_fkey  foreign key (shipping_country_id) references public.shipping_country_rates(shipping_country_id),
	constraint agreementid_fkey foreign key (agreementid) references public.shipping_agreement(agreementid));

insert into public.shipping_info
	select distinct
		sh.shippingid,
		sh.vendorid,
		sh.payment_amount,
		sh.shipping_plan_datetime,
		st.transfer_type_id,
		sct.shipping_country_id,
		sa.agreementid
	from public.shipping sh
	left join public.shipping_transfer st
		on st.transfer_type = (regexp_split_to_array(sh.shipping_transfer_description, ':+'))[1]::text
			and st.transfer_model = (regexp_split_to_array(sh.shipping_transfer_description, ':+'))[2]::text
	left join public.shipping_country_rates sct
		on sct.shipping_country = sh.shipping_country
	left join public.shipping_agreement sa
		on sa.agreementid::text = (regexp_split_to_array(sh.vendor_agreement_description, ':+'))[1];
--- select * from public.shipping_info  limit 10


--- shipping_status
create table if not exists public.shipping_status
	(shippingid		 					int8,
	status								text,
	state								text,
	shipping_start_fact_datetime		timestamp,
	shipping_end_fact_datetime			timestamp,
	constraint shippingid_pkey primary key (shippingid));

insert into public.shipping_status (shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
	with md as (select
			shippingid,
			max(case when state = 'booked' then state_datetime else null end) as max_start,
			max(case when state = 'recieved' then state_datetime else null end) as max_end
		from public.shipping
		group by shippingid)
	select
	t.shippingid,
	t.status,
	t.state,
	md.max_start as shipping_start_fact_datetime,
	md.max_end as shipping_end_fact_datetime
	from
		(select distinct on (sh.shippingid)
			sh.shippingid,
			sh.status,
			sh.state
		from public.shipping sh
		order by sh.shippingid, sh.state_datetime desc)t
	join md on md.shippingid=t.shippingid;
--- select * from shipping_status limit 10


--- shipping_datamart

create view public.shipping_datamart as
	select
		si.shippingid,
		si.vendorid,
		st.transfer_type,
		(ss.shipping_end_fact_datetime::date - ss.shipping_start_fact_datetime::date) as full_day_at_shipping,
		case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0 end as is_delay,
		case when ss.status = 'finished' then 1 else 0 end as is_shipping_finish,
		case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then ss.shipping_end_fact_datetime::date - si.shipping_plan_datetime::date else 0 end as delay_day_at_shipping,
		si.payment_amount,
		(si.payment_amount*(scr.shipping_country_base_rate+sa.agreement_rate+st.shipping_transfer_rate)) as vat,
		si.payment_amount*sa.agreement_commission as profit
	from public.shipping_info si
	join public.shipping_transfer st
		on si.transfer_type_id = st.transfer_type_id
	join public.shipping_status ss
		on si.shippingid = ss.shippingid
	join public.shipping_country_rates scr
		on si.shipping_country_id = scr.shipping_country_id
	join public.shipping_agreement sa
		on si.agreementid = sa.agreementid
--- select * from public.shipping_datamart
