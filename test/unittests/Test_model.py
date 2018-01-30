import unittest
import datetime
import hashlib
import context
from ReportingFramework import Report, Execution, Record, Column

# Tests for class Report


class TestGetId(unittest.TestCase):

    def test_logic_get_id(self):
        report = Report()
        report._id = 0
        self.assertEqual(report.get_id(), 0)

    def test_type_get_id(self):
        """The return type of the function should be the same than the DB"""
        report = Report()
        report._id = 0
        self.assertEqual(type(report.get_id()), type(0))


class TestGetLastReportPairs(unittest.TestCase):

    def test_logic_get_last_report_pairs(self):
        executions = []
        report = Report()

        for i in range(0, 100):
            executions.append(Execution(execution_mode="delta",
                                        execution_date=datetime.datetime.now()
                                                       - datetime.timedelta(days=-i)))
        report.execution = executions

        last_execution, last_execution_1 = report.get_last_report_pairs()

        self.assertEqual(last_execution.execution_date, executions[-1].execution_date)
        self.assertEqual(last_execution_1.execution_date, executions[-2].execution_date)

# Tests for class Execution


class TestAddRecord(unittest.TestCase):

    def test_add_record(self):
        execution = Execution()
        execution.records = []
        execution.records_lkp = {}

        execution.add_record(Record())

        self.assertEqual(len(execution.records), 1)
        self.assertEqual(len(execution.records_lkp), 1)


class TestGetRecordWithId(unittest.TestCase):

    def test_get_record_with_id(self):
        record = Record(record_id="test")
        execution = Execution()
        execution.add_record(record)
        execution.add_record(Record(record_id="NotTheOneIWant"))

        self.assertEqual(execution.get_record_with_id("test").id, record.id)


# Tests for class Column

class TestColumnIsEqual(unittest.TestCase):

    def test_is_equal_string(self):
        self.assertTrue(Column(name="first_name", value="Emmanuel")
                        .is_equal(Column(name="first_name", value="Emmanuel")))

        self.assertFalse(Column(name="first_name", value="Emmanuel")
                        .is_equal(Column(name="first_name", value="Jule")))

    def test_is_equal_int(self):
        self.assertTrue(Column(name="age", value=1)
                        .is_equal(Column(name="age", value=1)))

        self.assertFalse(Column(name="age", value=1)
                         .is_equal(Column(name="age", value=2)))


class TestColumnToString(unittest.TestCase):

    def test_column_to_string(self):
        self.assertTrue(Column(name="first_name", value="Emmanuel").to_string(),
                        'fist_name : Emmanuel')


# Tests for class Record

class TestHashRecord(unittest.TestCase):

    def test_hash_record(self):
        columns = [Column(name="name", value="TEST"),
                   Column(name="street", value="somewhere in Zurich"),
                   Column(name="age", value="10")]

        string_to_hash = "10TESTsomewhere in Zurich"

        a_record = Record(record_id="1", columns=columns)
        a_record.hash_record()

        self.assertEqual(a_record.record_hash,
                         hashlib.md5(string_to_hash).hexdigest())


class TestRecordIsEqual(unittest.TestCase):

    def test_record_is_equal_slow(self):
        columns = [Column(name="name", value="TEST"),
                   Column(name="street", value="somewhere in Zurich"),
                   Column(name="age", value="10")]

        a_record = Record(record_id="1", columns=columns)
        another_record = Record(record_id="2", columns=columns)

        self.assertTrue(a_record.is_equal(another_record, mode='slow'))

    def test_record_is_equal_fast(self):
        columns = [Column(name="name", value="TEST"),
                   Column(name="street", value="somewhere in Zurich"),
                   Column(name="age", value="10")]

        a_record = Record(record_id="1", columns=columns)
        another_record = Record(record_id="2", columns=columns)

        self.assertTrue(a_record.is_equal(another_record, mode='fast'))

    def test_record_is_equal_force_no_hash(self):
        columns = [Column(name="name", value="TEST"),
                   Column(name="street", value="somewhere in Zurich"),
                   Column(name="age", value="10")]

        a_record = Record(record_id="1", columns=columns)
        another_record = Record(record_id="2", columns=columns)

        # This will force the method to use the slow mode and compare column by column
        a_record.record_hash = ''
        another_record.record_hash = ''

        self.assertTrue(a_record.is_equal(another_record))


class TestAppendColumn(unittest.TestCase):

    def test_append_column(self):

        columns = [Column(name="name", value="TEST"),
                   Column(name="street", value="somewhere in Zurich"),
                   Column(name="age", value="10")]

        a_record = Record(record_id="1", columns=columns)

        a_record.append_column(name="answer", value="yes")

        self.assertIn("answer", [col.name for col in a_record.columns])


if __name__ == '__main__':
    unittest.main()
