import hashlib


class Report(object):
    """Report class is the report entity and the configuration"""
    def __init__(self, name=None, query=None, mode=None, columns=None, columns_mapping=None, separator=',',
                 file_name='not set'):
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

    def describe(self):
        print(self.name,
              self.query,
              self.mode,
              self.columns,
              self.columns_mapping,
              len(self.execution))

    def get_last_report_pairs(self):
        sorted_execution = sorted(self.execution, key=lambda an_execution: an_execution.execution_date)
        return sorted_execution[len(sorted_execution)-1], sorted_execution[len(sorted_execution)-2]

    def pprint(self):
        last_execution, last_execution_n_1 = self.get_last_report_pairs()
        print("Content of last execution: ")
        # Display header
        print(self.separator.join(self.columns))
        # Display content
        for record in last_execution.records:
            if record:
                print(self.separator.join([column.value for column in record.columns if column]))

    def write_to_file(self):
        f = open(self.file_name, 'w')
        # To do
        print("Writing last execution to "+self.file_name)
        # Get the last execution self.execution[-1]

        # Write headers (business column) with the separator
        columns_for_writing = [self.columns_mapping[col] for col in self.columns]
        columns_for_writing.append('crud type')
        f.write(self.separator.join(columns_for_writing)+'\n')

        # Write data with the separator
        for row in self.execution[-1].generated_records:
            f.write(row.to_string(self.separator)+'\n')

        f.close()
        return None


class Execution(object):
    """Execution class contains information related to an execution of the report"""
    def __init__(self, execution_date=None, execution_mode=None, records=None):
        super(Execution, self).__init__()
        self.execution_date = execution_date
        self.execution_mode = execution_mode

        self.records = []  # This holds the record from the data source
        self.generated_records = []  # This holds the record from the delta execution, if full then equals

        if records:
            self.records = records

    def get_record_with_id(self, record_id):
        return filter(lambda a_record: (a_record and a_record.id == record_id), self.records)[0]

    def describe(self):
        print("#####################")
        print("Execution date: "+self.execution_date.strftime("%Y-%m-%d %H:%M:%S"))
        print("Contains records: ")
        for record in self.records:
            if record:
                print("record id: "+str(record.id))
                record.show()


class Column(object):
    def __init__(self, name, value):
        super(Column, self).__init__()
        self.name = name
        self.value = value

    def is_equals(self, a_column):
        return self.value == a_column.value

    def to_string(self):
        return self.name+" : "+self.value


class Record(object):
    def __init__(self, record_id=None, columns=None, record_type='', record_hash=''):
        super(Record, self).__init__()
        self.id = record_id
        self.record_hash = record_hash
        self.columns = []
        self.record_type = record_type

        if columns:
            self.columns = columns

    def is_equals(self, a_record, mode='slow'):
        is_equals = True

        if mode == 'slow':
            for column in self.columns:
                for a_record_column in a_record.columns:
                    if a_record_column.name == column.name:
                        if not a_record_column.is_equals(column):
                            is_equals = False
                            print("Record with id: "
                                  + self.id
                                  + " "
                                  + column.name
                                  + " changed value from "
                                  + a_record_column.value
                                  + " to "
                                  + column.value)
        else:
            if self.record_hash != a_record.record_hash:
                is_equals = False

        return is_equals

    def show(self):
        for column in self.columns:
            if column:
                print(column.name+" : "+column.value)

    def to_string(self, separator=','):
        return separator.join([column.value for column in self.columns])

    def hash_record(self):
        column_sorted_by_name = sorted(self.columns, key=lambda l: l.name)
        string_to_hash = ''.join([column.value for column in column_sorted_by_name if column.name != id])
        self.record_hash = hashlib.md5(string_to_hash).hexdigest()

    def append_column(self, name, value):
        self.columns.append(Column(name, value))
