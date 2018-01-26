-- Clean database
DELETE FROM "Column"       ;
DELETE FROM Record         ;
DELETE FROM Execution      ;
DELETE FROM Report_Columns ;
DELETE FROM Report         ;

-- Create Report entity
insert into Report(id, name, report_query, mode, file_name, separator)
values (
1,
'test',
'select id, first_name, last_name, age from customer;',
'delta',
'/Users/manu/Temporaire/report.txt',
','
) ;

-- Define columns of the report and mapping business name
insert into Report_Columns(id, report_id, sql_name, business_name, business_key) values
(1, 1, 'id', 'id', 1),
(2, 1, 'first_name', 'customer_first_name', 0),
(3, 1, 'last_name' , 'customer_last_name', 0),
(4, 1, 'age', 'customer_age', 0) ;
