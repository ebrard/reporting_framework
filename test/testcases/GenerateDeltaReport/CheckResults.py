file_report = "/Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/report_2.csv"
file_status = "/Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/file_status.dat"

f_r = open(file_report, 'r')
f_s = open(file_status, 'r')

reference_value = {}

for line in f_s:
    r_id, status = line.split(' ')[0], line.split(' ')[1].replace('\n', '')
    reference_value[r_id] = status

for line in f_r:
    row = line.split(',')
    r_id = row[0]
    r_status = row[-1].replace('\n', '')

    if r_id != 'id':
        assert (r_status == reference_value[r_id])
