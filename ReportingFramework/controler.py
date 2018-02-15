"""

This module implements functions that interact directly with the model.
This does not contain any end-user facing functions

"""
import copy
import datetime
from model import Column
from model import Record
from model import Execution
import controler_db as ctrl_persist


def parse_query_result(query_result, this_execution, columns, columns_mapping):
    """Reads and parses data source query result and add it to
    existing execution instance"""

    columns_in_business_key = []

    for column_name, column_definition in columns_mapping.items():
        if "is_business_key" in column_definition:
            if column_definition["is_business_key"] == '1':
                columns_in_business_key.append(str(column_name))

    for row in query_result:
        # Construct the business key (record.id)
        # this_record = Record(row[0])

        # New method
        business_key = []
        for a_column in columns_in_business_key:
            business_key.append((a_column, row[a_column]))

        business_key.sort(key=lambda column_tuple: column_tuple[0])
        record_id = [column_tuple[1] for column_tuple in business_key]
        if len(record_id) == 1:
            record_id = str(record_id[0])
        else:
            record_id = ''.join(record_id)

        this_record = Record(record_id=record_id)

        # End of new method for business key
        this_record.record_type = 'source'  # This record was read from a report query

        # This approach is independent of the column order in the SQL defined
        # in the report configuration
        for a_column in columns:
            a_column = str(a_column)

            this_column = Column(a_column,
                                 str(row[a_column]),
                                 columns_mapping[a_column]["is_used_for_compare"]
                                 )

            this_record.columns.append(this_column)

        this_record.hash_record()
        this_execution.add_record(this_record)

        # j = 0
        # for column in row:
        #     this_column = Column(columns[j],
        #                          str(column), # We handle only string value type
        #                          columns_mapping[columns[j]]["is_used_for_compare"])
        #     this_record.columns.append(this_column)
        #     j += 1
        # this_record.hash_record()
        # this_execution.add_record(this_record)
        # i += 1


def generate_business_columns(record, columns_mapping):
    """Generates columns mapping dictionary by changing column name in the Column object"""
    for column in record.columns:
        if column.name in [value for value in list(columns_mapping.keys())]:
            column.name = columns_mapping[column.name]["business_name"]


def generate_delta(report, mode="slow"):
    """Generate delta report between current and previous data source state"""
    print "Info : Starting comparing records "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_report, last_report_1 = report.get_last_report_pairs()

    if last_report and last_report_1:

        last_report_ids = [str(element.id) for element in last_report.records if element]
        last_report_1_ids = [str(element.id) for element in last_report_1.records if element]

        for row in last_report.records:

            if row:
                if str(row.id) in last_report_1_ids:
                    # Updated
                    current_row = last_report.get_record_with_id(str(row.id))
                    previous_row = last_report_1.get_record_with_id(str(row.id))

                    if not current_row.is_equal(previous_row, mode=mode):
                        generated_row = copy.deepcopy(row)
                        generated_row.append_column("crud type", "U")
                        generated_row.record_type = 'generated'
                        last_report.generated_records.append(generated_row)

                    else:
                        generated_row = copy.deepcopy(row)
                        generated_row.append_column("crud type", "S")
                        generated_row.record_type = 'generated'
                        last_report.generated_records.append(generated_row)
                else:
                    # Created
                    generated_row = copy.deepcopy(row)
                    generated_row.append_column("crud type", "C")
                    generated_row.record_type = 'generated'
                    last_report.generated_records.append(generated_row)

        deleted_ids = [element for element in last_report_1_ids
                       if element and element not in last_report_ids]

        for deleted_id in deleted_ids:
            # Deleted
            row = last_report_1.get_record_with_id(deleted_id)
            generated_row = copy.deepcopy(row)
            generated_row.record_type = 'generated'
            generated_row.append_column("crud type", "D")
            last_report.generated_records.append(generated_row)

    else:
        if last_report:
            print "Info : No existing execution, creating a full report"
            for row in last_report.records:
                generated_row = copy.deepcopy(row)

                generated_row.append_column("crud type", "C")
                generated_row.record_type = 'generated'
                last_report.generated_records.append(generated_row)
        else:
            print "Error no report to generate"
            exit(1)

    print "Info : End of records comparison "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def generate_full(report):
    """Generate full report even if delta is the default configuration"""
    print "Full report requested"
    last_report = report.get_last_report_pairs()[0]  # No interest in previous execution
    for row in last_report.records:
        generated_row = copy.deepcopy(row)
        generated_row.record_type = 'generated'
        last_report.generated_records.append(generated_row)


def run_an_execution(report_name, execution_mode):
    """
    Run an execution of a report in a specified mode

    :param report_name:
    :param execution_mode:
    :return:
    """

    with ctrl_persist.get_backend_connection() as conn:
        # Retrieve report
        this_report = ctrl_persist.get_report_by_name(report_name, conn)
        # Instantiate an execution

        if not execution_mode:
            execution_mode = this_report.mode
        else:
            this_report.mode = execution_mode

        query_result = None
        execution = None

        # Connect to data database
        with ctrl_persist.get_source_connection() as conn_source:
            cur_source = conn_source.cursor()

            # Run report query on this database
            execution = Execution(datetime.datetime.now(), execution_mode)
            query_result = cur_source.execute(this_report.query).fetchall()

        # Parse query result
        # Associate parsed data with the execution
        parse_query_result(query_result, execution, this_report.columns, this_report.columns_mapping)
        this_report.execution.append(execution)  # Add report execution to the report

        # Close connection to data database
        conn_source.close()

        # Generate delta or full
        if this_report.mode == 'delta':
            generate_delta(this_report)
        else:
            generate_full(this_report)

        # Write data to backend db
        ctrl_persist.persist_execution(execution, this_report.get_id(), conn)

        # Write report data to file
        this_report.write_to_file()

        return {
            "file_name":this_report.file_name
        }
