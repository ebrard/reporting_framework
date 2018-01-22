from model import Report
from model import Execution
from model import Column
from model import Record
from controler import parse_query_result
from controler import generate_delta


import datetime

my_report = Report()

my_report.query = "select id, name, age from test ;"
my_report.name = "my report"
my_report.mode = "delta"
my_report.columns = ["id", "name", "age"]
my_report.columns_mapping = { "id":"customer_id", "name":"customer_name", "age":"customer_age" }
my_report.separator = ','

this_execution = Execution(execution_date = datetime.datetime.now(), execution_mode = my_report.mode)

# Data retrieved from database

# Execution 1
query_result = [ ["0", "jeff", "0"], ["1", "jeremy", "10"], ["34", "jeremy", "10"] ]

# Parse data retrieved from database
parse_query_result(query_result, this_execution, my_report.columns)
my_report.execution.append(this_execution)

my_report.pprint()

# Execution 2
this_execution = Execution(execution_date = datetime.datetime.now(), execution_mode = my_report.mode)

query_result = [ ["0", "jeff", "0"], ["1", "jeremy", "20"], ["2", "emmanuel", "31"] ]

parse_query_result(query_result, this_execution, my_report.columns)

my_report.execution.append(this_execution)

my_report.pprint()

print("DELTA")
generate_delta(my_report, this_execution)