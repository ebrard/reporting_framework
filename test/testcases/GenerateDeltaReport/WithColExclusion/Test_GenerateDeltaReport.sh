#!/usr/bin/env bash

function exit_on_error {
    if [ $? != 0 ]; then
        echo "Error"
        exit 1
    fi
}

root_path="/Users/manu/Documents/ReportingFramework"
test_path="${root_path}/test/testcases/GenerateDeltaReport/WithColExclusion/"
db_path="/Users/manu/Documents/ReportingFramework/test"

nb_records=10
nb_cycle=1

echo "Creating report in the backend database"
sqlite3 ${root_path}/db/backend.db < ${test_path}/CreateReport.sql
exit_on_error

echo "Create data in the source database"
sqlite3 ${db_path}/data.db "delete from customer ;"
exit_on_error

python3 ${test_path}/CreateOrModifyData.py create ${nb_records}
exit_on_error

for i in {1..${nb_cycle}}
do

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

sleep 5
done

rm -f created.pickle
rm -f file_status.dat
rm -f report*.csv