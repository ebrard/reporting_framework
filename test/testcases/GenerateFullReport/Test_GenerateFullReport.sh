#!/usr/bin/env bash

function exit_on_error {
    if [ $? != 0 ]; then
        echo "Error"
        exit 1
    fi
}

root_path="/Users/manu/Documents/ReportingFramework/"
test_path="${root_path}/test/testcases/GenerateFullReport/"
db_path="/Users/manu/Documents/ReportingFramework/test"

echo "Creating report in the backend database"
sqlite3 ${root_path}/db/backend.db < ${test_path}/CreateReport.sql
exit_on_error

echo "Create data in the source database"
sqlite3 ${db_path}/data.db "drop table if exists customer ;"
sqlite3 ${db_path}/data.db "create table customer(id int, first_name varchar(200), last_name varchar(200), \
age int, birthdate date) ;"
sqlite3 ${db_path}/data.db "delete from customer ;"
exit_on_error

python3 ${test_path}/CreateData.py
exit_on_error

echo "Running Report in full mode"
python ${root_path}/ReportingFramework/reporting_framework.py execute --name test --mode full
exit_on_error