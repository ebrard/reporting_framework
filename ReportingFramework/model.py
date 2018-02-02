"""

This module implements the logical data model of the reporting framework

"""

import hashlib


class Report(object):
    """Report class is the report entity and the configuration holder of a report"""
    def __init__(self, name=None, query=None, mode=None,
                 columns=None, columns_mapping=None, separator=',',
                 file_name='not set', report_id=None):
        super(Report, self).__init__()
        self.name = name
        self.query = query
        self.mode = mode
        self.columns = []  # This is an issue for persistence (ok fixed in sql model)
        self.columns_mapping = {}
        self.execution = []
        self.separator = separator
        self.file_name = file_name
        self._id = -1

        if columns:
            self.columns = columns

        if columns_mapping:
            self.columns_mapping = columns_mapping

        if report_id:
            self._id = report_id

    def get_id(self):
        """Return the database id of the report"""
        return self._id

    def describe(self):
        """Display information about the report on stdout"""
        print(self.name,
              self.query,
              self.mode,
              self.columns,
              self.columns_mapping,
              len(self.execution))

    def get_last_report_pairs(self):
        """Return the last two executions of a report, used for comparison"""
        current_execution = None
        previous_execution = None

        if len(self.execution) > 1:
            sorted_execution = sorted(self.execution,
                                      key=lambda an_execution: an_execution.execution_date)
            list_size = len(sorted_execution)
            current_execution = sorted_execution[list_size-1]
            previous_execution = sorted_execution[list_size-2]
        else:
            if len(self.execution) == 1:
                print "Warning : one execution only to sort"
                current_execution = self.execution[0]
            else:
                print "Error : no execution to return"

        return current_execution, previous_execution

    def pprint(self):
        """Display information about the last execution"""
        last_execution = self.get_last_report_pairs()[0]
        print "Content of last execution: "
        # Display header
        print self.separator.join(self.columns)
        # Display content
        for record in last_execution.records:
            if record:
                print self.separator.join([column.value for column in record.columns if column])

    def write_to_file(self):
        """Write the last execution to file on disk"""
        file_out = open(self.file_name, 'w')

        # Write headers (business column) with the separator
        columns_for_writing = [self.columns_mapping[col]["business_name"] for col in self.columns]
        columns_for_writing.append('crud type')
        file_out.write(self.separator.join(columns_for_writing)+'\n')

        # Write data with the separator
        print "Info : Writing last execution to " + self.file_name
        for row in self.execution[-1].generated_records:
            file_out.write(row.to_string(self.separator)+'\n')

        file_out.close()


class Execution(object):
    """Execution class contains information related to an execution of the report"""
    def __init__(self, execution_date=None, execution_mode=None, records=None):
        super(Execution, self).__init__()
        self.execution_date = execution_date
        self.execution_mode = execution_mode

        self.records = []  # holds the record from the data source
        self.generated_records = []  # holds the record from the delta execution
        self.records_lkp = {}

        if records:
            self.records = sorted(records, key=lambda a_record: a_record.id)

            for record in records:
                self.records_lkp[str(record.id)] = record

    def add_record(self, record):
        """Add a record to an execution"""
        if record:
            self.records.append(record)
            self.records_lkp[str(record.id)] = record

    def get_record_with_id(self, record_id):
        """Return a record with the specified business key, only one expected"""

        if str(record_id) not in self.records_lkp:
            print "Error no matching record with: " + str(record_id)
            exit(1)

        return self.records_lkp[str(record_id)]

    def describe(self):
        """Display information about an execution"""
        print "#####################"
        print "Execution date: "+self.execution_date.strftime("%Y-%m-%d %H:%M:%S")
        print "Contains records: "
        for record in self.records:
            if record:
                print "record id: "+str(record.id)
                record.show()


class Column(object):
    """Column class holds the name and value of a column which belongs to a record"""
    def __init__(self, name, value, is_used_for_compare=True):
        super(Column, self).__init__()
        self.name = name
        self.value = value
        self.is_used_for_compare = is_used_for_compare

    def is_equal(self, a_column):
        """Assert if column a equals column b based on value"""
        is_col_equal = False

        if self.is_used_for_compare != a_column.is_used_for_compare:
            raise ValueError

        if self.is_used_for_compare:
            is_col_equal = self.value == a_column.value
        else:
            is_col_equal = True
        return is_col_equal

    def to_string(self):
        """Return a string representation of a column"""
        return self.name+" : "+self.value


class Record(object):
    """This class contains record level information and hash value for faster compare"""
    def __init__(self, record_id=None, columns=None, record_type='', record_hash=''):
        super(Record, self).__init__()
        self.id = record_id
        self.record_hash = record_hash
        self.columns = []
        self.record_type = record_type

        if columns:
            self.columns = columns
            self.hash_record()

    def is_equal(self, a_record, mode='slow'):
        """Assert if record a equals record b based on column compare"""
        is_equals = True

        if mode == 'slow' or (self.record_hash == '' and a_record.record_hash == ''):
            for column in self.columns:
                for a_record_column in a_record.columns:
                    if a_record_column.name == column.name:
                        if not a_record_column.is_equal(column):
                            is_equals = False
                            # print("Info : Record with id: "
                            #      + str(self.id)
                            #      + " "
                            #      + column.name
                            #      + " changed value from "
                            #      + a_record_column.value
                            #      + " to "
                            #      + column.value)
        else:
            if self.record_hash != a_record.record_hash:
                is_equals = False

        return is_equals

    def show(self):
        """Display all columns of a record"""
        for column in self.columns:
            if column:
                print "Info : Record id "+str(self.id)+" "+column.name+" : "+column.value

    def to_string(self, separator=','):
        """String representation of a record used for hashing"""
        return separator.join([column.value for column in self.columns])

    def hash_record(self):
        """Hash a string representation of a record for comparison"""
        column_sorted_by_name = sorted(self.columns, key=lambda l: l.name)

        string_to_hash = ''.join([column.value for column in column_sorted_by_name
                                  if column.name != id and column.is_used_for_compare == 1]
                                 )

        self.record_hash = hashlib.md5(string_to_hash).hexdigest()

    def append_column(self, name, value):
        """Add a column to an existing record (append at the end)"""
        self.columns.append(Column(name, value))
