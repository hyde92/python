#! /usr/bin/python

import sys
import os

os.system("export ODBCINI=/opt/teradata/client/ODBC_64/odbc.ini")
os.system("export ODBCINST=/opt/teradata/client/ODBC_64/odbcinst.ini")
os.system("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/teradata/client/16.20/odbc_64/lib")

import teradata
import pandas as pd
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('/tmp/run_logs/watchdog.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
def error_handler(type, value, tb):
    logger.exception("Errors: {0}".format(str(value)))
sys.excepthook = error_handler

logger.info('Starting')

host,username,password = 'mozart.vip.ebay.com','mkral', 'Asfradsrghh1s##f'
#Make a connection

udaExec = teradata.UdaExec(
    odbcLibPath="/opt/teradata/client/16.20/odbc_64/lib/libodbc.so",
    appName="test",
    version="1.0",
    logConsole=False,
    configureLogging=False
)


connect = udaExec.connect(method="odbc", system=host ,username=username, password=password,driver="Teradata Database ODBC Driver 16.20",charset='UTF16')

cursor = connect.cursor()

logger.info('Connected to DB')

volatile_a = ("""
create multiset volatile table nue_campaigns_a as (
select distinct(src_cmpgn_code), incntv_cd, max(i.cmpgn_sent_dt) as maxsentdt_coup
from  prs_restricted_v.dw_eip_incntv_lkp as b
join prs_restricted_v.dw_eip_incntv_cntct a
on a.eip_incntv_id = b.eip_incntv_id
join dw_cmc_instnc as i on i.instnc_id = a.instnc_id
join dw_cmc_lkp as cmc on i.cmc_id = cmc.cmc_id
where i.cmpgn_sent_dt >= date - 60
and i.chnl_id = 1
and incntv_cd in
(
		select incntv_cd
		from p_nue_fraud_t.nj_incentives_dash where INCNTV_EXPRN_DATE > current_date
		
) group by 1,2) with data primary index(src_cmpgn_code) on commit preserve rows;
""")

volatile_b = ("""
create multiset volatile table nue_campaigns_b as (
select distinct(src_cmpgn_code), max(i.cmpgn_sent_dt) as maxsentdt_camp
from  prs_restricted_v.dw_eip_incntv_lkp as b
join prs_restricted_v.dw_eip_incntv_cntct a
on a.eip_incntv_id = b.eip_incntv_id
join dw_cmc_instnc as i on i.instnc_id = a.instnc_id
join dw_cmc_lkp as cmc on i.cmc_id = cmc.cmc_id
where i.cmpgn_sent_dt >= date - 60
and i.chnl_id = 1
and incntv_cd in
(
		select incntv_cd
		from p_nue_fraud_t.nj_incentives_dash where INCNTV_EXPRN_DATE > current_date
		
) group by 1) with data primary index(src_cmpgn_code) on commit preserve rows;
""")


volatile_c = ("""
create multiset volatile table nue_campaigns as (
select a.src_cmpgn_code, incntv_cd, maxsentdt_coup, maxsentdt_camp
, q.last_run
from nue_campaigns_a as a
join nue_campaigns_b as b on a.src_cmpgn_code = b.src_cmpgn_code
left join (
  select
  max(starttime (date)) as last_run
  , trim(substr(queryband, 23, 6)) as campcode
  from dw_monitor_viewsx.qrylog_hist
  where 1=1
  and appID = 'UNICA'
  and querytext is not null
  and querytext not like any ('SET QUERY_BAND = ''PROD_CAMPAIGNCODE=%')
  and trim(substr(queryband, 23, 6)) in (select
   distinct(src_cmpgn_code) as campcode from nue_campaigns_a
   )
  and starttime (date) >= current_date - 60
  and logdate (date) >= current_date - 60
  group by 2
) as q on q.campcode = a.src_cmpgn_code
where q.last_run > current_date - 2

) with data primary index(src_cmpgn_code) on commit preserve rows;
""")

volatile_d = ("""
create multiset volatile table nue_campaigns_show as (
select distinct(src_cmpgn_code), d.incntv_cd, segm_name, strtok(country, '.', 2) as country
, case when coupon_day = '' then 'Other' else coupon_day end as coupon_day
from  prs_restricted_v.dw_eip_incntv_lkp as b
join prs_restricted_v.dw_eip_incntv_cntct a
on a.eip_incntv_id = b.eip_incntv_id
join dw_cmc_instnc as i on i.instnc_id = a.instnc_id
join dw_cmc_lkp as cmc on i.cmc_id = cmc.cmc_id
join access_views.dw_cmc_segm_lkp  as seg on seg.segm_id = i.segm_id and seg.segm_name not like all ('%SEED%', '%FRAUD%', '%NONCPN%')
join p_nue_fraud_t.nj_incentives_dash as d on d.incntv_cd = b.incntv_cd
where i.cmpgn_sent_dt >= date - 60
and i.chnl_id = 1
and b.incntv_cd in
(
		select incntv_cd
		from p_nue_fraud_t.nj_incentives_dash where cmc_max_end_date > current_date
		
) ) with data primary index(src_cmpgn_code) on commit preserve rows;
""")

drop_watchdog_2m = ("""drop table p_nue_fraud_t.watchdog_2m;""")

create_watchdog_2m = ("""
create multiset table p_nue_fraud_t.watchdog_2m as (
select
instnc.CMPGN_RUN_DT +1 as cmpgn_sent_dt
, cmc.src_cmpgn_code
, seg.segm_name
, count(distinct(contact.user_id)) as user_count
, sum(case when contact.open_cnt > 0 then 1 else 0 end)/2 as opens
from dw_cmc_cntct as contact
join dw_cmc_instnc as instnc
on contact.instnc_id = instnc.instnc_id
and instnc.chnl_id = 1
join dw_cmc_lkp as cmc
on instnc.cmc_id = cmc.cmc_id
join nue_campaigns as nue on nue.src_cmpgn_code = cmc.src_cmpgn_code ------------------- 
join access_views.dw_cmc_segm_lkp  as seg on seg.segm_id = instnc.segm_id and seg.segm_name not like all ('%SEED%', '%FRAUD%', '%NONCPN%')
where instnc.CMPGN_RUN_DT + 1 >= maxsentdt_camp - 60
group by 1,2,3) with data primary index(cmpgn_sent_dt, segm_name);
""")


drop_watchdog_3m = ("""drop table p_nue_fraud_t.watchdog_3m;""")

create_watchdog_3m = ("""
create multiset table p_nue_fraud_t.watchdog_3m as (SELECT 
	count(CNTCT.USER_ID) as coupsent, 
	INSTNC_T.CMPGN_RUN_DATE + 1 as cmpgn_sent_dt, 
	EIP_LKP.INCNTV_CD,
	cmc.src_cmpgn_code,
		seg.SEGM_NAME
		FROM 
	PRS_RESTRICTED_V.DW_EIP_INCNTV_CNTCT AS CNTCT
	INNER JOIN
		PRS_RESTRICTED_V.DW_EIP_INCNTV_INSTNC as INSTNC_T
		ON
			CNTCT.INSTNC_ID = INSTNC_T.INSTNC_ID  		
	INNER JOIN 
		PRS_RESTRICTED_V.DW_EIP_INCNTV_LKP AS EIP_LKP
		ON 
			CNTCT.EIP_INCNTV_ID = EIP_LKP.EIP_INCNTV_ID
			join Access_Views.DW_CMC_SEGM_LKP as seg on seg.segm_id = INSTNC_T.segm_id
	join nue_campaigns as nue on nue.INCNTV_CD = EIP_LKP.INCNTV_CD ------------------- 
	join dw_cmc_instnc as i on i.instnc_id = CNTCT.instnc_id
join dw_cmc_lkp as cmc on i.cmc_id = cmc.cmc_id

WHERE 
	CNTCT.USER_ID < 2E9
and INSTNC_T.CMPGN_RUN_DATE + 1 >= maxsentdt_coup -60
group by INSTNC_T.CMPGN_RUN_DATE , EIP_LKP.INCNTV_CD, cmc.src_cmpgn_code, seg.SEGM_NAME
) with data primary index(cmpgn_sent_dt, INCNTV_CD);
""")

drop_watchdog_med_2m = ("""drop table p_nue_fraud_t.watchdog_med_2m;""")

create_watchdog_med_2m = ("""
create multiset table p_nue_fraud_t.watchdog_med_2m as (
select
b.src_cmpgn_code as campaign, a.segment, d.med_camp, a.med_segm, a.min_segm, a.max_segm
from nue_campaigns as b
left join (
		select
		src_cmpgn_code
		, segm_name as segment
		, median(user_count) as med_segm
		, med_segm - zeroifnull(STDDEV_SAMP(user_count))*2 as min_segm
		, med_segm + zeroifnull(STDDEV_SAMP(user_count))*2 as max_segm
		from p_nue_fraud_t.watchdog_2m
		group by 1,2
		) as a on b.src_cmpgn_code = a.src_cmpgn_code
join (
		select src_cmpgn_code, median(user_count) as med_camp
		from p_nue_fraud_t.watchdog_2m
		group by 1
) as d on d.src_cmpgn_code = a.src_cmpgn_code
) with data primary index(segment,campaign);
""")

drop_watchdog_med_3m = ("""drop table p_nue_fraud_t.watchdog_med_3m;""")

create_watchdog_med_3m = ("""
create multiset table p_nue_fraud_t.watchdog_med_3m as (
select INCNTV_CD, median(coupsent) as med_coup
		, med_coup - zeroifnull(STDDEV_SAMP(coupsent))*2 as min_coup
		, med_coup + zeroifnull(STDDEV_SAMP(coupsent))*2 as max_coup
from p_nue_fraud_t.watchdog_3m
group by INCNTV_CD
) with data primary index(incntv_cd);
""")

drop_watchdog_pre_log = ("""drop table p_nue_fraud_t.watchdog_pre_log;""")

create_watchdog_pre_log = ("""
create multiset table p_nue_fraud_t.watchdog_pre_log as (
select camp.dateo as CMPGN_SENT_DT, camp.src_cmpgn_code,incntv_cd,camp.segm_name, country, coupon_day
, coalesce(user_count,0) as user_count, coalesce(opens,0) as opens,coalesce(coupsent,0) as coupsent, med_camp
,med_coup, min_coup, max_coup
, med_segm, min_segm, max_segm
from
(
select dateo, xx.src_cmpgn_code, xx.segm_name, country, coupon_day, user_count, opens, med_camp, med_segm, min_segm, max_segm
from 
(
select cast(calendar_date as date) as dateo from sys_calendar.CALENDAR
where calendar_date  between current_date - 60 and current_date - 2
) as mtime
cross join nue_campaigns_show as xx
left join p_nue_fraud_t.watchdog_2m as t
on t.src_cmpgn_code = xx.src_cmpgn_code and t.segm_name = xx.segm_name and mtime.dateo = t.cmpgn_sent_dt
left join p_nue_fraud_t.watchdog_med_2m as m
	on m.campaign = xx.src_cmpgn_code and xx.segm_name = m.segment
group by 1,2,3,4,5,6,7,8,9,10,11
) as camp
left join
(
select dateo, xx.src_cmpgn_code, xx.incntv_cd,xx.segm_name, med_coup, min_coup, max_coup, coupsent
from
(
select cast(calendar_date as date) as dateo from sys_calendar.CALENDAR
where calendar_date  between current_date - 60 and current_date - 2
) as mtime
cross join nue_campaigns_show as xx
left join p_nue_fraud_t.watchdog_3m as u
on u.incntv_cd = xx.incntv_cd and mtime.dateo = u.cmpgn_sent_dt  and u.src_cmpgn_code = xx.src_cmpgn_code
and u.SEGM_NAME = xx.segm_name
left join p_nue_fraud_t.watchdog_med_3m as n
on n.incntv_cd = xx.incntv_cd
group by 1,2,3,4,5,6,7,8
) as coup
on camp.src_cmpgn_code = coup.src_cmpgn_code and camp.dateo =  coup.dateo and camp.segm_name = coup.segm_name
) with data primary index(CMPGN_SENT_DT,incntv_cd,src_cmpgn_code);
""")


#drop table p_nue_fraud_t.watchdog_log;
#create multiset table p_nue_fraud_t.watchdog_log as (
#	select row_number() over (order by CMPGN_SENT_DT desc, segm_name asc) as id
#, current_timestamp(0) + interval '9' hour as log_prg
#, CMPGN_SENT_DT, t.country
#, t.coupon_day
#, t.incntv_cd, src_cmpgn_code as campaign, segm_name as segment, user_count, opens, med_camp, med_segm, min_segm, max_segm,
#case when coalesce(t.user_count,0) not = 0 then
#(cast(fr.fraud_red as decimal(18,4))/cast(t.user_count as decimal(18,4)))*100
#else 0 end as fraud_pct
#, case
#	when t.user_count > 0 and opens = 0 then 'Not sending'
#		when t.user_count = 0 then 'No coupons issued'
#	when t.user_count > 0 and fraud_pct > 5 then 'Fraud spike'
#	when t.user_count < min_segm then 'Low volume'
#	when t.user_count > max_segm and t.coupon_day <> 'day 40' then 'High volume'
#	else 'x' end as Audience_check
#, cast(NULL as varchar (64) character set unicode not casespecific) as resolved
#, cast(1 as integer) as shouldrun
#	from
#p_nue_fraud_t.watchdog_pre_log as t
#left join
#	(select send_dt, coupon_day, incntv_cd, sum(users_sent) as users_sent, sum(redeemed) as redeemed, sum (fraud_red) as fraud_red
#	from  p_dbm_reporting_t.sends_dash
#	group by send_dt, coupon_day, incntv_cd) as fr
#	on fr.send_dt = CMPGN_SENT_DT and fr.INCNTV_CD = t.INCNTV_CD
#) with data primary index(id);


insert_watchdog_log = ("""
insert into p_nue_fraud_t.watchdog_log
select
id + (sel max(id) from p_nue_fraud_t.watchdog_log) as id
, log_prg , CMPGN_SENT_DT, country
, coupon_day
, incntv_cd, campaign, segment, user_count, opens, med_camp, med_segm, min_segm, max_segm,
fraud_pct, Audience_check, resolved, shouldrun
from (
select row_number() over (order by CMPGN_SENT_DT desc, segm_name asc) as id
, current_timestamp(0) + interval '9' hour as log_prg
, CMPGN_SENT_DT, t.country
, t.coupon_day
, t.incntv_cd, src_cmpgn_code as campaign, segm_name as segment, user_count, opens, med_camp, med_segm, min_segm, max_segm,
case when coalesce(t.user_count,0) not = 0 then
(cast(fr.fraud_red as decimal(18,4))/cast(t.user_count as decimal(18,4)))*100
else 0 end as fraud_pct
, case
	when t.user_count > 0 and opens = 0 then 'Not sending'
		when t.user_count = 0 then 'No coupons issued'
	when t.user_count > 0 and fraud_pct > 5 then 'Fraud spike'
	when t.user_count < min_segm then 'Low volume'
	when t.user_count > max_segm and t.coupon_day <> 'day 40' then 'High volume'
	else 'x' end as Audience_check
, cast(NULL as varchar (64) character set unicode not casespecific) as resolved
, cast(1 as integer) as shouldrun
	from
p_nue_fraud_t.watchdog_pre_log as t
left join
	(select send_dt, coupon_day, incntv_cd, sum(users_sent) as users_sent, sum(redeemed) as redeemed, sum (fraud_red) as fraud_red
	from  p_dbm_reporting_t.sends_dash
	group by send_dt, coupon_day, incntv_cd) as fr
	on fr.send_dt = CMPGN_SENT_DT and fr.INCNTV_CD = t.INCNTV_CD

	where CMPGN_SENT_DT > (select max(CMPGN_SENT_DT) from p_nue_fraud_t.watchdog_log)) as ccc;
""")

update_ended = ("""UPDATE p_nue_fraud_t.watchdog_log
SET shouldrun = 0,
resolved = 'Paused'
WHERE id in (
select id from (
select a.id, a.cmpgn_sent_dt, a.incntv_cd, a.campaign, a.segment, b.last_0
from p_nue_fraud_t.watchdog_log as a
join (
  select max(cmpgn_sent_dt) as last_0, incntv_cd, campaign, segment
  from p_nue_fraud_t.watchdog_log
  where shouldrun = 0
  group by 2,3,4
  ) as b
 on a.incntv_cd = b.incntv_cd
 and a.campaign = b.campaign
 and a.segment = b.segment
where a.cmpgn_sent_dt > b.last_0
)z);
""")


query = ("""sel CMPGN_SENT_DT,country,coupon_day,INCNTV_CD,campaign,segment,user_count,opens,med_segm,cast(fraud_pct as integer) as fraud_pct,Audience_check
from p_nue_fraud_t.watchdog_log 
where 1=1
and Audience_check <> 'x'
and med_segm > 9
and cmpgn_sent_dt between current_date - 15 and current_date - 2
and shouldrun = 1
and resolved is NULL

order by cmpgn_sent_dt desc, incntv_cd;""")




cursor.execute(volatile_a)
cursor.execute(volatile_b)
cursor.execute(volatile_c)
cursor.execute(volatile_d)
logger.info('Volatiles created')
cursor.execute(drop_watchdog_2m)
cursor.execute(create_watchdog_2m)
cursor.execute(drop_watchdog_3m)
cursor.execute(create_watchdog_3m)
cursor.execute(drop_watchdog_med_2m)
cursor.execute(create_watchdog_med_2m)
cursor.execute(drop_watchdog_med_3m)
cursor.execute(create_watchdog_med_3m)
cursor.execute(drop_watchdog_pre_log)
cursor.execute(create_watchdog_pre_log)
logger.info('Data prepared, inserting to data log')
cursor.execute(insert_watchdog_log)
cursor.execute(update_ended)
cursor.execute(query)

res = cursor.fetchall()
df = pd.DataFrame(res)
rowcount = df.shape[0]

logger.info('Data fetched, creating message')

if rowcount > 0:
    df.columns = ['Send_dt', 'Country', 'Group', 'Coupon', 'Campaign', 'Segment', 'User count', 'Opens', 'Median segm', 'Fraud percent', 'Result']
    df.set_index(['Send_dt', 'Country', 'Group', 'Coupon', 'Campaign', 'Segment', 'User count', 'Opens', 'Median segm', 'Fraud percent', 'Result'], inplace=True)
pass

#recipients = ['mkral@ebay.com']
recipients = ["mkral@ebay.com", "mhavlek@ebay.com", "rmahalaha@ebay.com", "jsvejda@ebay.com", "ablais@ebay.com", "driggins@ebay.com"]
to_addr = ", ".join(recipients)
msg = MIMEMultipart()
msg['Subject'] = "Watchdog test"
msg['From'] = 'ebaymonitor@noreply.com'
msg['To'] = to_addr


html = """\
<html>
<head>
</head>
<body bgcolor='#00b3b3'>
<table border='0' cellspacing='0' cellpadding='0' align='center' width='900'>
<tr>
	<td height="100" align="center">
	<span style="font-family: Consolas, 'Andale Mono', 'Lucida Console', 'Lucida Sans Typewriter', Monaco, 'Courier New', 'monospace'; font-size: 33; font-weight: 600">
NUE monitoring
	</span>
	</td>
</tr>
<tr>
	<td align="center" width="800">
	<span style="font-family: Consolas, 'Andale Mono', 'Lucida Console', 'Lucida Sans Typewriter', Monaco, 'Courier New', 'monospace'; font-size: 19">
Monitoring system for New User Engagement campaigns.<br>
The results are for unchecked anomalies in last 15 days.<br>
Parameters can be still a bit sensitive, we will adjust<br> that during this testing period.
	</span>
	</td>
</tr>
<tr>
	<td align="center" style="font-family: Consolas, 'Andale Mono', 'Lucida Console', 'Lucida Sans Typewriter', Monaco, 'Courier New', 'monospace'; font-size: 19">
	<span style="font-family: Consolas, 'Andale Mono', 'Lucida Console', 'Lucida Sans Typewriter', Monaco, 'Courier New', 'monospace'; font-size: 19; width: 800">
<br>{0}
	</span>
	</td>
</tr>
<tr>
	<td align="center">
	<span style="font-family: Consolas, 'Andale Mono', 'Lucida Console', 'Lucida Sans Typewriter', Monaco, 'Courier New', 'monospace'; font-size: 15">
	<br>
Please do not reply to this email.<br>
In case of any complications please contact<br>
		<a style='text-decoration:none; font-weight: 600' href='mailto:mkral@ebay.com?subject=Watchdog notification'>Michal Kral</a> or
		<a style='text-decoration:none; font-weight: 600' href='mailto:mhavlek@ebay.com?subject=Watchdog notification'>Milan Havlicek</a>.
<br><br>
Thank you and have a nice day!
	</span>
	</td>
</tr>
</table>
</body>
</html>
""".format(df.to_html())

part1 = MIMEText(html, 'html')
msg.attach(part1)

server = smtplib.SMTP('atom.corp.ebay.com')

logger.info('Message created, deciding send')

if rowcount > 0:
    server.sendmail(msg['From'], to_addr, msg.as_string())
    logger.info('Email sent')
pass

server.quit()
connect.commit()
cursor.close()
connect.close()

logger.info('Closed, done')