import unittest
import datetime
import context
from ReportingFramework import Report, Execution, Record, Column
from ReportingFramework import controler as ctrl


class TestParseQueryResult(unittest.TestCase):

    def test_parse_query_result(self):
        columns_definition = ["id", "first name", "last name", "age"]

        columns_mapping = {
            "id": {"is_business_key": 1, "is_used_for_compare": 0},
            "first name": {"is_business_key": 0, "is_used_for_compare": 1},
            "last name": {"is_business_key": 0, "is_used_for_compare": 1},
            "age": {"is_business_key": 0, "is_used_for_compare": 0}
        }

        query_result = [{"id": 1, "first name": "test", "last name": "Test", "age": 10},
                        {"id": 2, "first name": "test2", "last name": "Tes", "age": 90}]

        this_execution = Execution(execution_date=datetime.datetime.now(),
                                   execution_mode="delta")

        ctrl.parse_query_result(query_result,
                                this_execution,
                                columns=columns_definition,
                                columns_mapping=columns_mapping)

        # We expect record to be sorted in the same way they were feed
        i = 0
        for record in this_execution.records:
            self.assertItemsEqual([column.value for column in record.columns],
                             [str(col) for col in list(query_result[i].values())])
            i += 1


class TestGenerateBusinessColumns(unittest.TestCase):

    def test_generate_business_columns(self):
        columns_definition = ["id", "first name", "last name", "age"]

        columns_mapping = {
            "id": {"business_name": "id", "is_business_key": 1, "is_used_for_compare": 0},
            "first name": {"business_name": "First", "is_business_key": 0, "is_used_for_compare": 1},
            "last name": {"business_name": "Last", "is_business_key": 0, "is_used_for_compare": 1},
            "age": {"business_name": "AGE", "is_business_key": 0, "is_used_for_compare": 0}
        }

        columns_definition = ["id", "first name", "last name", "age"]

        query_result = [{"id": 1, "first name": "test", "last name": "Test", "age": 10},
                        {"id": 2, "first name": "test2", "last name": "Tes", "age": 90}]

        this_execution = Execution(execution_date=datetime.datetime.now(),
                                   execution_mode="delta")

        ctrl.parse_query_result(query_result,
                                this_execution,
                                columns=columns_definition,
                                columns_mapping=columns_mapping)

        for record in this_execution.records:
            # This will change the name of the column for the business name
            ctrl.generate_business_columns(record, columns_mapping)

            for col in record.columns:
                self.assertIn(col.name, [value["business_name"] for value in list(columns_mapping.values())])


if __name__ == '__main__':
    unittest.main()
