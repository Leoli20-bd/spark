select current_date() as cnt_dt,current_timestamp() as cnt_ts;

select date_format(current_date(),'yyyyMMdd') as dt_fm,date_format(current_timestamp(),'yyyyMMdd HH:mm:ss.SSS') as ts_fm,date_format('2022-06-15','yyyyMMdd') as str_dt_fm,date_format('20220615','yyyy-MM-dd') str_not_fm;

select to_date('20120615','yyyyMMdd') as str_dt, to_date(current_timestamp()) as tm_dt;

select date_add(current_date(),1) dt_add, date_sub('2022-06-22',-1) str_dt_sub;

select from_unixtime(1655910258,'yyyy-MM-dd HH:mm:ss') as bt_fm;

select to_timestamp('2022-06-23') as dt_tm,to_timestamp('20220623 12:00:12','yyyyMMdd HH:mm:ss') dt_fm_tm;


