from sortedcontainers import SortedList
import hashlib

class Report(object):
	"""docstring for Report"""
	def __init__(self, name = None, query = None, mode = None, columns = None, columns_mapping = None, separator = ',', file_name = 'not set'):
		super(Report, self).__init__()
		self.name = name
		self.query = query
		self.mode = mode
		self.columns = []
		self.columns_mapping = {}
		self.execution = []
		self.separator = separator
		self.file_name = file_name

	def describe(self):
		print(self.name, self.query, self.mode, self.columns, self.columns_mapping)

	def get_last_report_pairs(self):
		sorted_execution = sorted(self.execution, key=lambda an_execution: an_execution.execution_date)
		return ( sorted_execution[ len(sorted_execution) -1 ], sorted_execution[ len(sorted_execution) -2 ] )

	def pprint(self):
		last_execution, last_execution_n_1 = self.get_last_report_pairs()
		
		# Display header
		print( self.separator.join( self.columns ) )
		# Display content
		for record in last_execution.records:
			if record:
				print( self.separator.join( [ column.value for column in record.columns if column] ) )

class Execution(object):
	"""docstring for Execution"""
	def __init__(self, execution_date = None, execution_type = None, execution_mode = None, records = None):
		super(Execution, self).__init__()
		self.execution_date = execution_date
		self.execution_type = execution_type
		self.execution_mode = execution_mode
		
		self.records = [records] # This holds the record from the data source
		self.generated_records = [] # This holds the record from the delta execution, if full then equals

	def get_record_with_id(self, id):
		return filter(lambda a_record: ( a_record and a_record.id == id ), self.records)[0]

	def describe(self):
		print("#####################")
		print("Execution date: "+self.execution_date.strftime("%Y-%m-%d %H:%M:%S"))
		print("Constains records: ")
		for record in self.records:
			if record:
				record.show()

class Column(object):
	def __init__(self, name, value):
		super(Column, self).__init__()
		self.name = name
		self.value = value

	def is_equals(self, a_column):
		return (self.value == a_column.value)

class Record(object):
	def __init__(self, id = None, Column = None):
		super(Record, self).__init__()
		self.id = id
		self.record_hash = 'not set'
		self.columns = []
		self.hash_function = hashlib.md5()

		if Column:
			self.columns.append(Column)

	def is_equals(self,a_record):
		is_equals = True
		for column in self.columns:
			for a_record_column in  a_record.columns:

				if (a_record_column.name == column.name):
					if not a_record_column.is_equals(column):
						is_equals = False
		return is_equals


	def show(self):
		for column in self.columns:
			if column:
				print(column.name+" : "+column.value)

	def to_string(self):
		return ','.join( [ column.value for column in self.columns ] )

	def hash_record(self):
		string_to_hash = ','.join( [ column.value for column in self.columns if column.name != id ] )
		self.record_hash = hashlib.sha224(string_to_hash).hexdigest()

	def append_column(self, name, value):
		self.columns.append(Column(name, value))



