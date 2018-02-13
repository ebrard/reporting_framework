#!/usr/bin/env bash

function exit_on_error {
    if [ $? != 0 ]; then
        echo "Error"
        exit 1
    fi
}

root_path="/Users/manu/Documents/ReportingFramework"
test_path="${root_path}/test/testcases/GenerateDeltaReport/WithNewColumnAfterExecution"
db_path="/Users/manu/Documents/ReportingFramework/test"

nb_records=10
nb_cycle=3

echo "Creating report in the backend database"
sh ${root_path}/db/reset_db.sh
exit_on_error
sqlite3 ${root_path}/db/backend.db < ${test_path}/CreateReport.sql
exit_on_error

echo "Create data in the source database"
sqlite3 ${db_path}/data.db "delete from customer ;"
exit_on_error

python3 ${test_path}/CreateOrModifyData.py create ${nb_records}
exit_on_error


echo "Running Report in delta mode"
time python ${root_path}/ReportingFramework/reporting_framework.py execute --name test --mode delta
exit_on_error
mv /Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/report.csv \
/Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/report_1.csv

echo "Modify data in the source database"
python3 ${test_path}/CreateOrModifyData.py modify ${nb_records}
exit_on_error

echo "Running Report in delta mode"
time python ${root_path}/ReportingFramework/reporting_framework.py execute --name test --mode delta
exit_on_error
mv /Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/report.csv \
/Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/report_2.csv

echo "Checking report content after data change"
python CheckResults.py
exit_on_error

echo "Adding new columns"
#sqlite3 ${db_path}/data.db "alter table customer add is_good int default 0;"
sqlite3 ${root_path}/db/backend.db "insert into Report_Columns ( report_id, sql_name, business_name, is_business_key, is_used_for_compare) VALUES (1, 'is_good', 'Is Good customer', 0, 1)"
sqlite3 ${root_path}/db/backend.db "update Report set report_query='select id, first_name, last_name, age, is_good from customer' where id = 1"

echo "Running Report in delta mode"
time python ${root_path}/ReportingFramework/reporting_framework.py execute --name test --mode delta
exit_on_error
mv /Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/report.csv \
/Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/report_3.csv

rm -f ../created.pickle
rm -f file_status.dat
rm -f report*.csv