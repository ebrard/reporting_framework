drop table if exists Report ;
drop table if exists Execution ;
drop table if exists Record ;
drop table if exists Column ;
drop table if exists Report_Columns ;

create table Report (
    id integer primary key autoincrement,
    name varchar (500),
    report_query text, -- map to Class Report query variable
    mode varchar(300),
    file_name varchar(300),
    separator varchar(3)
) ;

create table Report_Columns (
    id integer primary key autoincrement,
    report_id int,
    sql_name varchar(300) not null,
    business_name varchar(300),
    FOREIGN KEY(report_id) REFERENCES Report(id)
) ;

create table Execution (
    id integer primary key autoincrement,
    report_id int,
    execution_date datetime not null,
    execution_mode varchar(300) not null,
    FOREIGN KEY(report_id) REFERENCES Report(id)
) ;

create table Record (
    id integer primary key autoincrement,
    execution_id int,
    record_hash text,
    record_type varchar(50) check(record_type in ('sql', 'framework')),
    FOREIGN KEY(execution_id) REFERENCES Execution(id)
) ;

create table Column(
    id integer primary key autoincrement,
    record_id int,
    name varchar (500),
    value text,
    FOREIGN KEY(record_id) REFERENCES Record(id)
) ;
