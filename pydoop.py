import resource
import os
import pydoop.hdfs as hdfs
from itertools import islice
import ssl
from impala.dbapi import connect
import os

number_of_lines = 5

os.system("export HADOOP_CONF_DIR=/etc/hadoop/conf/")


input_file = "/tmp/business/business.csv"

count = 0
data = []
line_data = []
data_split = {}
with hdfs.open(input_file,'r') as f:

	for line in f:
		count = count + 1
		line_data = line
		line_data = line_data.decode('utf8').split(',')
		# print (line_data)
		data.append(line_data)
		# print (line)


		if count == 15:
			break

# print (data[0])


conn = connect(host='worker2.valhalla.phdata.io',
              port=21050,
              use_ssl=True,
              database='default',
              user='sumit',
              kerberos_service_name='impala',
              auth_mechanism = 'GSSAPI')
cur = conn.cursor()
cur.execute('create table IF NOT EXISTS testsks LIKE sumitdata.business;')
cur.execute('describe extended testsks;')
print (cur.fetchall())

# cur.execute('select * from testsks')

# print (var_string)
# query_string = 'insert into testsks values(%s) ;' % var_string
# cur.execute(query_string, line)
# params = ['?' for item in data[1]]
# query = 'insert into testsks values(%s);', % [','.join(params)]
cur.execute('insert into testsks values(%s);', data[1])
result=cur.fetchall()
print (result)
