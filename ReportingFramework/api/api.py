#!flask/bin/python

from flask import Flask, jsonify, request, abort
import sqlite3
import datetime

import context
from ReportingFramework import controler_db as ctrl_db
from ReportingFramework import controler as ctrl
from ReportingFramework import Report, Execution

app = Flask(__name__)

backend_db_file = '/Users/manu/Documents/ReportingFramework/db/backend.db'


@app.route('/')
def index():
    return "Hello, World!"


@app.route('/reports', methods=['GET'])
def get_report():

    with sqlite3.connect(backend_db_file) as conn:
        conn.row_factory = sqlite3.Row
        reports = ctrl_db.get_all_report(conn)

    return jsonify([{"report_name": a_report.name,
                     "last_execution": a_report.execution[-1].execution_date
                     }
                    for a_report in reports])


@app.route('/reports/<string:report_name>', methods=['GET'])
def get_report_by_name(report_name):

    with sqlite3.connect(backend_db_file) as conn:
        conn.row_factory = sqlite3.Row
        report = ctrl_db.get_report_by_name(report_name, conn)

    return jsonify(
        {
            "report_name": report.name,
            "report_file": report.file_name,
            "report_query": report.query,
            "columns": report.columns,
            "column_definition": report.columns_mapping
        }
    )


@app.route('/executions/<string:report_name>', methods=['GET'])
def get_executions_for_report(report_name):

    with sqlite3.connect(backend_db_file) as conn:
        conn.row_factory = sqlite3.Row
        executions = ctrl_db.get_report_past_execution(report_name, conn)

    return jsonify(
        {"report_name": report_name},
        [
          {
              "execution_id": an_execution["execution_id"],
              "execution_date": an_execution["execution_date"],
              "number_of_records": an_execution["records_nb"]
          } for an_execution in executions]
    )


@app.route('/executions', methods=['POST'])
def execute_report_sync():

    if not request.json:
        abort(400)

    execution_mode = request.json["mode"]
    report_name = request.json["report_name"]

    with sqlite3.connect(backend_db_file) as conn:
        conn.row_factory = sqlite3.Row
        # Retrieve report
        this_report = ctrl_db.get_report_by_name(report_name, conn)
        # Instantiate an execution

        if not execution_mode:
            execution_mode = this_report.mode
        else:
            this_report.mode = execution_mode

        # Connect to data database
        conn_source = sqlite3.connect('/Users/manu/Documents/ReportingFramework/test/data.db')
        conn_source.row_factory = sqlite3.Row

        cur_source = conn_source.cursor()

        # Run report query on this database
        execution = Execution(datetime.datetime.now(), execution_mode)
        query_result = cur_source.execute(this_report.query).fetchall()

        # Parse query result
        # Associate parsed data with the execution
        ctrl.parse_query_result(query_result, execution, this_report.columns, this_report.columns_mapping)
        this_report.execution.append(execution)  # Add report execution to the report

        # Close connection to data database
        conn_source.close()

        # Generate delta or full
        if this_report.mode == 'delta':
            ctrl.generate_delta(this_report)
        else:
            ctrl.generate_full(this_report)

        # Write data to backend db
        ctrl_db.persist_execution(execution, this_report.get_id(), conn)

        # Write report data to file
        this_report.write_to_file()

        return jsonify(
            "Execution done at "
            + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            + " "
            + this_report.file_name
            + " has been written."
        )


if __name__ == '__main__':
    app.run(debug=True)