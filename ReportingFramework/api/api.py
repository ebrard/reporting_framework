#!flask/bin/python

from flask import Flask, jsonify, request, abort
from flask_swagger import swagger
import sqlite3
import datetime

import context
from ReportingFramework import controler_db as ctrl_db
from ReportingFramework import controler as ctrl
from ReportingFramework import Report, Execution

app = Flask(__name__)

backend_db_file = '/Users/manu/Documents/ReportingFramework/db/backend.db'


@app.route('/spec')
def spec():
    swag = swagger(app)
    swag['info']['version'] = "1.0"
    swag['info']['title'] = "Reporting framework API"
    return jsonify(swag)


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

    print("Info: execution started")

    execution_info = ctrl.run_an_execution(
        request.json["report_name"],
        request.json["mode"])

    return jsonify(
        "Execution done at "
        + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        + " "
        + execution_info["file_name"]
        + " has been written."
        )


if __name__ == '__main__':
    app.run(debug=True)