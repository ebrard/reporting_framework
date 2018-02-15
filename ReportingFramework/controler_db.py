"""

Map the Class data model with the DB data model

"""
import sqlite3
import datetime
from dateutil import parser
from model import Report
from model import Execution
from model import Record
from model import Column

# Source database


def get_source_connection():
    """
    Get a connection to the source database

    :return: connection
    """
    conn_source = sqlite3.connect('/Users/manu/Documents/ReportingFramework/test/data.db')
    conn_source.row_factory = sqlite3.Row

    return conn_source

# Persistence storage function


def get_backend_connection():
    """
    Get a connection to the backend database

    :return: connection
    """
    conn_source = sqlite3.connect('/Users/manu/Documents/ReportingFramework/db/backend.db')
    conn_source.row_factory = sqlite3.Row

    return conn_source

# Read function


def now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_executions_for_report(conn):
    """
    :param conn: (Connection) : DB connection to use
    :return: [Execution]
    """
    executions = []

    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sql_get_executions_for_report = "SELECT execution.id as exec_id, execution_date, " \
                                        "execution_mode from Execution inner join Report " \
                                        "on Report.id = Execution.report_id " \
                                        "where report.name = '%s' ;"

    query_result = cursor.execute(sql_get_executions_for_report).fetchall()

    for row in query_result:
        executions.append(Execution(execution_date=row["execution_date"],
                                    execution_mode=row["execution_date"]))

    return executions


def get_all_report(conn):
    """
    This function returns a list of reports with one execution instance (the last one if any)

    Args:
        conn (Connection) : DB connection to use
    Return:
        [Report] : A list of reports (not all dependencies are instantiated)

    """
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    reports = []

    sql_get_all_report = 'select name, max(execution_date) as last_execution from Report left join Execution ' \
                         'on Execution.report_id = Report.id ;'

    query_result = cursor.execute(sql_get_all_report).fetchall()

    for row in query_result:
        report = Report(str(row[0]))
        if row[1]:
            report.execution.append(Execution(parser.parse(row[1])))
        reports.append(report)

    return reports


def get_columns_by_record_id(record_id, conn):
    """This function returns a column list for a specific record_id"""
    columns = []

    cursor = conn.cursor()

    sql_columns_by_record_id = "select name, value, is_used_for_compare from Column where record_id = '%s';"

    query_result = cursor.execute(sql_columns_by_record_id % record_id).fetchall()

    for row in query_result:
        columns.append(Column(row["name"],
                              str(row["value"]),
                              str(row["is_used_for_compare"]))
                       )

    return columns


def get_records_by_execution_id(execution_id, conn):
    """This function returns a records list of Execution for a specific execution id"""

    records = []
    # generated_records = []

    cursor = conn.cursor()

    sql_records_by_execution_id = "select id, business_key, execution_id, record_hash, record_type from Record where " \
                                  "execution_id = '%s' and record_type in ('source', 'generated') "

    # SQL Records
    query_result = cursor.execute(sql_records_by_execution_id % execution_id).fetchall()

    for row in query_result:
        record = Record(str(row["business_key"]), get_columns_by_record_id(row["id"], conn))

        # The business key could be a simple int but we handle it as string since python is not
        # statically typed.

        record.record_hash = row["record_hash"]
        record.record_type = row["record_type"]
        records.append(record)

    # Generated Records (record_type = 'framework')
    # No Use-Case
    # but could be used to re-produce a generated report file at any time

    if not records:
        print "Error : no record found for execution "+execution_id

    return records


def get_execution_by_report_id(report_id, conn, execution_id=None):
    """This function returns a single instance of Execution for a specific report"""
    cursor = conn.cursor()
    execution = []

    if not execution_id:
        sql_execution_by_report_id = "select id, execution_date, execution_mode from Execution where " \
                                     "report_id = '%s' order by id desc limit 1"
    else:
        sql_execution_by_report_id = "select id, execution_date, execution_mode from Execution where " \
                                     "report_id = '%s' and Execution.id = %s limit 1"

    # Fetch report information from persistence
    if not execution_id:
        query_result = cursor.execute(sql_execution_by_report_id % report_id).fetchone()
    else:
        query_result = cursor.execute(sql_execution_by_report_id % (report_id, execution_id)).fetchone()

    if query_result:
        execution_id = query_result["id"]
        records = get_records_by_execution_id(execution_id, conn)
        source_record = [a_record for a_record in records if a_record.record_type == 'source']
        generated_record = [a_record for a_record in records if a_record.record_type == 'generated']

        this_execution = Execution(parser.parse(query_result["execution_date"]),
                                   query_result["execution_mode"],
                                   source_record)

        this_execution.generated_records = generated_record

        execution = [this_execution]

    if execution is None:
        print("Warning: no execution found for report id " + str(report_id))

    return execution


def get_report_by_name(report_name, conn, execution_id=None):
    cursor = conn.cursor()

    sql_report_by_name = "select id, name, report_query, mode, file_name, separator from Report where name = '%s';"
    sql_report_columns_by_id = "select sql_name, business_name, is_used_for_compare, is_business_key " \
                               "from Report_Columns where report_id = '%s';"

    # Fetch report information from persistence
    print "Info : Retrieving report from the database "+now()

    query_result = cursor.execute(sql_report_by_name % report_name).fetchone()

    if not query_result:
        print("Error : report with name '%s' not found" % report_name)
        exit(1)

    report_id = query_result["id"]
    report_name = query_result["name"]
    report_query = query_result["report_query"]
    report_mode = query_result["mode"]
    report_file_name = query_result["file_name"]
    report_separator = query_result["separator"]

    # Fetch column definitions
    query_result = cursor.execute(sql_report_columns_by_id % report_id).fetchall()

    columns_mapping = {}
    columns = []

    for row in query_result:
        columns.append(row["sql_name"])
        columns_mapping[row["sql_name"]] = {"business_name": row["business_name"]}
        columns_mapping[row["sql_name"]]["is_used_for_compare"] = str(row["is_used_for_compare"])
        columns_mapping[row["sql_name"]]["is_business_key"] = str(row["is_business_key"])

    report = Report(report_name,
                    report_query,
                    report_mode,
                    columns,
                    columns_mapping,
                    report_separator,
                    report_file_name,
                    report_id)

    # Retrieve last Execution or the one requested bw user
    if not execution_id:
        execution = get_execution_by_report_id(report_id, conn)
    else:
        execution = get_execution_by_report_id(report_id, conn, execution_id)

    if execution:
        report.execution = execution
    else:
        print "Warning : empty exectution"
        report.execution = []

    print "Info : Retrieval done "+now()
    return report


def get_report_past_execution(report_name, conn):
    """

    :param report_name:
    :param conn:
    :return: [ {} ]
    """

    sql_get_report_past_execution = " SELECT Report.id as report_id, name, " \
                                    " Execution.id as execution_id, execution_date, " \
                                    " (select count(*) from Record " \
                                    " where Record.execution_id = Execution.id " \
                                    " and record_type = 'generated') as records_nb" \
                                    " FROM Report inner join Execution on report.id = execution.report_id" \
                                    " where Report.name = '%s'"

    cursor = conn.cursor()
    query_result = cursor.execute(sql_get_report_past_execution % report_name).fetchall()

    return query_result


# Persist functions


def persist_columns(columns, record_id, conn):
    """Persist a list of columns to the storage"""

    sql_persist_columns = "insert into Column(record_id, name, value, is_used_for_compare) values (?, ?, ?, ?)"
    cursor = conn.cursor()

    list_of_columns_tuples = [(record_id, col.name, col.value, col.is_used_for_compare) for col in columns]

    cursor.executemany(sql_persist_columns, list_of_columns_tuples)
    conn.commit()


def persist_records(records, execution_id, conn):
    """Persist a list of records to the storage with their associated columns"""
    cursor = conn.cursor()

    for a_record in records:
        record_tuple = (execution_id, a_record.id, a_record.record_hash, a_record.record_type)

        cursor.execute('insert into Record(execution_id, business_key, record_hash, record_type) values (?, ?, ?, ?);',
                  record_tuple)

        # Get id of that record from the database to persist columns
        record_id = cursor.lastrowid
        persist_columns(a_record.columns, record_id, conn)

    conn.commit()


def persist_execution(execution, report_id, conn):
    """Persist an execution of a report to the storage with dependencies"""
    cursor = conn.cursor()

    # An execution is for SQL : report_id, execution_date, execution_mode
    # With for the class model records and generated_records

    cursor.execute('insert into Execution(report_id, execution_date, execution_mode) values (?, ?, ?)',
              (report_id,
               execution.execution_date,
               execution.execution_mode
               )
              )
    execution_id = cursor.lastrowid

    if execution.records:
        print("Info : persisting records type source")
        persist_records(execution.records, execution_id, conn)
    else:
        print("Warning : no sql record to persist")

    if execution.generated_records:
        print("Info : persisting records type generated")
        persist_records(execution.generated_records, execution_id, conn)
    else:
        print("Warning : no framework record to persist")

    conn.commit()


def persist_report_column_mapping(column_mapping, report_id, conn):
    return None


def persist_report(report, conn):
    """Persist report configuration"""
    cursor = conn.cursor()

    cursor.execute('insert into Report(name, report_query, mode, file_name, separator) values (?, ?, ?, ?, ?)',
              (
                  report.name,
                  report.query,
                  report.mode,
                  report.file_name,
                  report.separator
              )
              )
    report_id = cursor.lastrowid

    for key, value in report.columns_mapping.iteritems():
        cursor.execute('insert into Report_Columns(report_id, sql_name, business_name) values (?, ?, ?)',
                  (
                      report_id,
                      key,
                      value
                  )
                  )

    conn.commit()

    return report_id
