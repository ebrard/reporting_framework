from model import Report
from model import Execution
from model import Column
from model import Record


def parse_query_result(query_result, this_execution, columns):
    i = 0
    for row in query_result:
        this_record = Record(row[0])
        j = 0
        for column in row:
            # print("processing: "+columns[j]+" "+column)
            this_column = Column( columns[j], column )
            this_record.columns.append(this_column)
            j += 1
        this_record.hash_record()
        print(this_record.record_hash)
        this_execution.records.append(this_record)
        i += 1


def generate_delta(report):

    last_report, last_report_1 = report.get_last_report_pairs()

    last_report_ids = [element.id for element in last_report.records if element]
    last_report_1_ids = [element.id for element in last_report_1.records if element]

    for row in last_report.records:

        # To-Do: Fix issue with None assignment
        if row:
            if row.id in last_report_1_ids:
                # Updated

                current_row = last_report.get_record_with_id(row.id)
                previous_row = last_report_1.get_record_with_id(row.id)

                if not current_row.is_equals(previous_row, mode='fast'):
                    row.append_column("crud type", "U")
                    print(row.to_string())
                else:
                    row.append_column("crud type", "S")
                    print(row.to_string())
            else:
                # Created
                row.append_column("crud type", "C")
                print(row.to_string())

    deleted_ids = [element for element in last_report_1_ids if element and element not in last_report_ids]

    for deleted_id in deleted_ids:
        # Deleted
        row = last_report_1.get_record_with_id(deleted_id)
        row.append_column("crud type", "D")
        print(row.to_string())
