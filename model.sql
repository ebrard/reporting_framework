drop table if exists Report ;
drop table if exists Execution ;
drop table if exists Record ;
drop table if exists Column ;

create table Report (
    id int primary key,
    name varchar (500),
    report_query text, -- map to Class Report query variable
    mode varchar(300),
    file_name varchar(300),
    separator varchar(3)
) ;

create table Report_Columns (
    id int,
    report_id int,
    sql_name varchar(300) not null,
    business_name varchar(300),
    FOREIGN KEY(report_id) REFERENCES Report(id)
) ;

create table Execution (
    id int primary key,
    report_id int,
    execution_date datetime not null,
    execution_mode varchar(300) not null,
    FOREIGN KEY(report_id) REFERENCES Report(id)
) ;

create table Record (
    id int primary key,
    execution_id int,
    record_hash text,
    record_type varchar(50) check(record_type in ('sql', 'framework')),
    FOREIGN KEY(execution_id) REFERENCES Execution(id)
) ;

create table Column(
    id int primary key,
    record_id int,
    name varchar (500),
    value text,
    FOREIGN KEY(record_id) REFERENCES Record(id)
) ;

commit ;