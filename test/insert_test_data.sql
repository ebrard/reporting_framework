DELETE FROM Column ;
DELETE FROM Record ;
DELETE FROM Execution ;
DELETE FROM Report_Columns ;
DELETE FROM Report ;

insert into Report(id, name, report_query, mode, file_name, separator)
values (
1,
'test',
'selet * from dumb;',
'delta',
'/Users/manu/Temporaire/report.txt',
','
) ;

insert into Report_Columns(id, report_id, sql_name, business_name) values
(
1,
1,
'age',
'customer_age'
) ;

insert into Report_Columns(id, report_id, sql_name, business_name) values
(
2,
1,
'name',
'customer_name'
) ;

insert into Execution(id, report_id, execution_date, execution_mode) values
(
1,
1,
'2018-01-23 00:00:00',
'delta'
);

insert into Record(id, execution_id, record_hash, record_type) values
(
1,
1,
'not set',
'sql'
) ;

insert into Column(id, record_id, name, value) values
(
1,
1,
'age',
'11'
) ;

insert into Column(id, record_id, name, value) values
(
2,
1,
'name',
'Lucien'
) ;