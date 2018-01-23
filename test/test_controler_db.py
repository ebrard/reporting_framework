import sys
sys.path.insert(0, '/Users/manu/Documents/Reporting_Framework')

import sqlite3
import datetime
import controler_db as cdb

from model import Report
from model import Column
from model import Record
from model import Execution

conn = sqlite3.connect('/Users/manu/Documents/Reporting_Framework/backend.db')
conn.row_factory = sqlite3.Row

# Test functions

# Retrieves data from know database test dataset
# get_columns_by_record_id
print("test get_columns_by_record_id")
columns = cdb.get_columns_by_record_id(1, conn)

for column in columns:
    print(column.to_string())

# get_records_by_execution_id
print("test get_records_by_execution_id")
records = cdb.get_records_by_execution_id(1, conn)

for record in records:
    record.show()

# get_execution_by_report_id
print("test get_execution_by_report_id")
executions = cdb.get_execution_by_report_id(1, conn)

for execution in executions:
    execution.describe()

# get_report_by_name
print("test get_report_by_name")
report = cdb.get_report_by_name("test", conn)
report.describe()
report.pprint()

# Persist data from new instance

# Persist column test
columns = [Column("test_name", "test_jean"), Column("test_age", "2")]
cdb.persist_columns(columns, 1, conn)

# Persist record test
records = []
for i in range(2, 10):
    records.append(Record(i, columns, 'sql'))

cdb.persist_records(records, 1, conn)

# Persist report test
my_report = Report()

my_report.query = "select id, name, age from test ;"
my_report.name = "my report"
my_report.mode = "delta"
my_report.file_name = "/Users/manu/Temporaire/report.txt"
my_report.columns = ["id", "name", "age"]
my_report.columns_mapping = {"id": "customer_id", "name": "customer_name", "age": "customer_age"}
my_report.separator = ','

report_id = cdb.persist_report(my_report, conn)

# Persist execution test
# To-do

this_execution = Execution(execution_date=datetime.datetime.now(), execution_mode=my_report.mode)

cdb.persist_execution(this_execution, report_id, conn)

conn.close()
