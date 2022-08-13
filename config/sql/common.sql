select current_date() as cnt_dt,current_timestamp() as cnt_ts;

select date_format(current_date(),'yyyyMMdd') as dt_fm,date_format(current_timestamp(),'yyyyMMdd HH:mm:ss.SSS') as ts_fm,date_format('2022-06-15','yyyyMMdd') as str_dt_fm,date_format('20220615','yyyy-MM-dd') str_not_fm;

select to_date('20120615','yyyyMMdd') as str_dt, to_date(current_timestamp()) as tm_dt;

select date_add(current_date(),1) dt_add, date_sub('2022-06-22',-1) str_dt_sub;

select from_unixtime(1655910258,'yyyy-MM-dd HH:mm:ss') as bt_fm;

select to_timestamp('2022-06-23') as dt_tm,to_timestamp('20220623 12:00:12','yyyyMMdd HH:mm:ss') dt_fm_tm;

SELECT array_contains(array(1, 2, 3), 2);

SELECT array_distinct(array(1, 2, 3, null, 3));

SELECT array_except(array(1, 2, 3), array(1, 3, 5));

SELECT array_intersect(array(1, 2, 3,3), array(1, 3, 5,3));

SELECT array_join(array('hello', 'world'), ' ');

select array_position(array(3,2,1,0),array_min(array(3,2,1,0))) as arr_pst;

SELECT array_sort(array(5, 6, 1), (left, right) -> case when left < right then -1 when left > right then 1 else 0 end) as arr_s;

SELECT array_sort(array('bc', 'ab', 'dc'), (left, right) -> case when left is null and right is null then 0 when left is null then -1 when right is null then 1 when left < right then 1 when left > right then -1 else 0 end) as arr_s;

select current_database() as cur_db;

SELECT java_method('java.util.UUID', 'fromString', 'a5cf6c42-0c85-418f-af6c-3e4e5b1328f2');

select rand(0) as rand ;


SELECT split('oneAtwoBthreeC', '[ABC]', -1);

SELECT str_to_map('a:1,b:2,c:3', ',', ':');
