import argparse
import sqlite3
import datetime

from controler_db import get_report_by_name, persist_execution, get_all_report
from controler import parse_query_result, generate_delta, generate_full
from model import Execution

backend_db_file = '/Users/manu/Documents/Reporting_Framework/db/backend.db'

# ###################### #
# Parse command line arg #
# ###################### #

cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument('action', help='List all available reports for execution')
cmd_parser.add_argument('--name', help='Report name to execution')
cmd_parser.add_argument('--mode', help='Specify mode for this execution')
cmd_parser.add_argument('--id', help='Specify execution to re-generate')

args = cmd_parser.parse_args()

execution_mode = args.mode
ui_action = args.action
report_name = args.name

# ###################### #
# List available report  #
# ###################### #

if ui_action == "list":

    # Connect to backend
    conn = sqlite3.connect(backend_db_file)
    # Retrieve all report with the last execution
    available_reports = get_all_report(conn)
    # Display report name and last execution
    for a_report in available_reports:
        message_string = "Report: " + a_report.name
        if a_report.execution:
            message_string += ", last execution: " + a_report.execution[-1].execution_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            message_string += ", no execution yet"
        print(message_string)
    # Close backend connection
    conn.close()


# ###################### #
# Run a report execution #
# ###################### #

if ui_action == "execute":

    # Connect to backend
    conn = sqlite3.connect(backend_db_file)
    conn.row_factory = sqlite3.Row

    # Retrieve report
    this_report = get_report_by_name(report_name, conn)
    # Instantiate an execution

    if not execution_mode:
        execution_mode = this_report.mode
    else:
        this_report.mode = execution_mode

    # Connect to data database
    conn_source = sqlite3.connect('/Users/manu/Documents/Reporting_Framework/test/data.db')
    cur_source = conn_source.cursor()

    # Run report query on this database
    execution = Execution(datetime.datetime.now(), execution_mode)
    query_result = cur_source.execute(this_report.query).fetchall()

    # Parse query result
    # Associate parsed data with the execution
    parse_query_result(query_result, execution, this_report.columns)
    this_report.execution.append(execution)  # Add report execution to the report

    # Close connection to data database
    conn_source.close()

    # Generate delta or full
    if this_report.mode == 'delta':
        generate_delta(this_report)
    else:
        generate_full(this_report)

    # Write data to backend db
    persist_execution(execution, this_report.get_id(), conn)

    # Write report data to file
    this_report.write_to_file()

    # Close backend connection
    conn.close()

# ################## #
# Regenerate report  #
# ################## #

if ui_action == "regenerate":

    # Connect to backend
        # Retrieve a specific execution by date
        # Write execution to file
    print("Not implemented yet")
