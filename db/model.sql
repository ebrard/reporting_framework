drop table if exists Report ;
drop table if exists Execution ;
drop table if exists Record ;
drop table if exists Column ;
drop table if exists Report_Columns ;

create table Report (
    id integer primary key autoincrement,
    name varchar (500) unique,
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
    is_business_key int check(is_business_key in (0,1)) default 0 not null,
    is_used_for_compare int check (is_used_for_compare in (0,1)) default 1 not null,
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
    business_key varchar(800), -- Needs to be persisted from the engine
    execution_id int,
    record_hash text,
    record_type varchar(50) check(record_type in ('source', 'generated')), -- change for source, delta, full
    FOREIGN KEY(execution_id) REFERENCES Execution(id)
) ;

create table Column(
    id integer primary key autoincrement,
    record_id int,
    is_used_for_compare int check (is_used_for_compare in (0,1)) default 1 not null,
    name varchar (500),
    value text,
    FOREIGN KEY(record_id) REFERENCES Record(id)
) ;
