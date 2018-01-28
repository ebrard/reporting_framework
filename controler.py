from model import Column
from model import Record
import copy
import datetime

def parse_query_result(query_result, this_execution, columns):
    i = 0
    for row in query_result:
        this_record = Record(row[0])
        this_record.record_type = 'source'  # This record was read from a report query
        j = 0
        for column in row:
            this_column = Column(columns[j], str(column))  # We handle only string value type
            this_record.columns.append(this_column)
            j += 1
        this_record.hash_record()
        this_execution.add_record(this_record)
        i += 1


def generate_business_columns(record, columns_mapping):
    for column in record.columns:
        if column.name in columns_mapping:
            column.name = columns_mapping[column.name]


def generate_delta(report):
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

                    if not current_row.is_equal(previous_row, mode='fast'):
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

        deleted_ids = [element for element in last_report_1_ids if element and element not in last_report_ids]

        for deleted_id in deleted_ids:
            # Deleted
            row = last_report_1.get_record_with_id(deleted_id)
            generated_row = copy.deepcopy(row)
            generated_row.record_type = 'generated'
            generated_row.append_column("crud type", "D")
            last_report.generated_records.append(generated_row)

    else:
        if last_report:
            print("Info : No existing execution, creating a full report")
            for row in last_report.records:
                generated_row = copy.deepcopy(row)

                generated_row.append_column("crud type", "C")
                generated_row.record_type = 'generated'
                last_report.generated_records.append(generated_row)
        else:
            print("Error no report to generate")
            exit(1)

    print "Info : End of records comparison "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def generate_full(report):
    print("Full report requested")
    last_report, last_report_1 = report.get_last_report_pairs()
    for row in last_report.records:
        generated_row = copy.deepcopy(row)
        generated_row.record_type = 'generated'
        last_report.generated_records.append(generated_row)
