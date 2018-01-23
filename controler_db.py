"""Map the Class data model with the DB data model"""
from model import Report
from model import Execution
from model import Record
from model import Column
from dateutil import parser


def get_columns_by_record_id(record_id, conn):
    """This function returns a column list for a specific record_id"""
    columns = []

    c = conn.cursor()

    sql_columns_by_record_id = "select name, value from Column where record_id = '%s';"

    query_result = c.execute(sql_columns_by_record_id % record_id).fetchall()

    for row in query_result:
        columns.append(Column(row["name"], row["value"]))

    return columns


def get_records_by_execution_id(execution_id, conn):
    """This function returns a records list of Execution for a specific execution id"""

    records = []
    # generated_records = []

    c = conn.cursor()

    sql_records_by_execution_id = "select id, execution_id, record_hash, record_type from Record where " \
                                  "execution_id = '%s' and record_type = 'sql'"

    # SQL Records
    query_result = c.execute(sql_records_by_execution_id % execution_id).fetchall()

    for row in query_result:
        records.append(Record(row["id"], get_columns_by_record_id(row["id"], conn)))

        # row["record_hash"] # not implemented --+ Need to do +--
        # row["record_type"] # not implemented

    # Generated Records
    # No Use-Case

    return records


def get_execution_by_report_id(report_id, conn):
    """This function returns a single instance of Execution for a specific report"""
    c = conn.cursor()

    sql_execution_by_report_id = "select id, execution_date, execution_mode from Execution where " \
                                 "report_id = '%s' order by id desc limit 1"

    # Fetch report information from persistence
    query_result = c.execute(sql_execution_by_report_id % report_id).fetchone()

    execution_id = query_result["id"]
    records = get_records_by_execution_id(execution_id, conn)

    return [Execution(parser.parse(query_result["execution_date"]),
            query_result["execution_mode"],
            records)]


def get_report_by_name(report_name, conn):
    c = conn.cursor()

    sql_report_by_name = "select id, name, report_query, mode, file_name, separator from Report where name = '%s';"
    sql_report_columns_by_id = "select sql_name, business_name from Report_Columns where report_id = '%s';"

    # Fetch report information from persistence
    query_result = c.execute(sql_report_by_name % report_name).fetchone()

    report_id = query_result["id"]
    report_name = query_result["name"]
    report_query = query_result["report_query"]
    report_mode = query_result["mode"]

    # Fetch column definitions
    query_result = c.execute(sql_report_columns_by_id % report_id).fetchall()

    columns_mapping = {}
    columns = []

    for row in query_result:
        columns.append(row["sql_name"])
        columns_mapping[row["sql_name"]] = row["business_name"]

    report = Report(report_name, report_query, report_mode, columns, columns_mapping)

    # Retrieve last Execution
    report.execution = get_execution_by_report_id(report_id, conn)

    return report


def persist_columns(columns, record_id, conn):
    """Persist a list of columns to the storage"""

    sql_persist_columns = "insert into Column(record_id, name, value) values (?, ?, ?)"
    c = conn.cursor()

    list_of_columns_tuples = [(record_id, col.name, col.value) for col in columns]

    c.executemany(sql_persist_columns, list_of_columns_tuples)
    conn.commit()


def persist_records(records, execution_id, conn):
    """Persist a list of records to the storage with their associated columns"""
    c = conn.cursor()

    for a_record in records:
        record_tuple = (execution_id, a_record.record_hash, a_record.record_type)
        c.execute('insert into Record(execution_id, record_hash, record_type) values (?, ?, ?);', record_tuple)

        # Get id of that record from the database to persist columns
        record_id = c.lastrowid
        persist_columns(a_record.columns, record_id, conn)

    conn.commit()


def persist_execution(execution, report_id, conn):
    """Persist an execution of a report to the storage with dependencies"""
    c = conn.cursor()

    # An execution is for SQL : report_id, execution_date, execution_mode
    # With for the class model records and generated_records

    c.execute('insert into Execution(report_id, execution_date, execution_mode) values (?, ?, ?)',
              (report_id,
               execution.execution_date,
               execution.execution_mode
               )
              )
    execution_id = c.lastrowid

    if len(execution.records) > 0 :
        persist_records(execution.records, execution_id, conn)

    if len(execution.generated_records) > 0 :
        persist_records(execution.generated_records, execution_id, conn)

    conn.commit()


def persist_report_column_mapping(column_mapping, report_id, conn):
    return None


def persist_report(report, conn):
    """Persist report configuration"""
    c = conn.cursor()

    c.execute('insert into Report(name, report_query, mode, file_name, separator) values (?, ?, ?, ?, ?)',
              (
                  report.name,
                  report.query,
                  report.mode,
                  report.file_name,
                  report.separator
              )
              )
    report_id = c.lastrowid

    for k, v in report.columns_mapping.iteritems():
        c.execute('insert into Report_Columns(report_id, sql_name, business_name) values (?, ?, ?)',
                  (
                      report_id,
                      k,
                      v
                  )
                  )

    conn.commit()

    return report_id

