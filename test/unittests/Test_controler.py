import unittest
import datetime
import context
from ReportingFramework import Report, Execution, Record, Column
from ReportingFramework import controler as ctrl


class TestParseQueryResult(unittest.TestCase):

    def test_parse_query_result(self):
        columns_definition = ["id", "first name", "last name", "age"]

        query_result = [[1, "test", "Test", 10],
                        [2, "first", "LAST", "100"]]

        this_execution = Execution(execution_date=datetime.datetime.now(),
                                   execution_mode="delta")

        ctrl.parse_query_result(query_result, this_execution, columns=columns_definition)

        # We expect record to be sorted in the same way they were feed
        i = 0
        for record in this_execution.records:
            self.assertEqual([column.value for column in record.columns],
                             [str(col) for col in query_result[i]])
            i += 1


if __name__ == '__main__':
    unittest.main()
