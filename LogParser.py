import SparkSessionWrapper
import re
import sys
import logging

logging.basicConfig(filename='logs/sparkjob.log', level=logging.INFO, format='%(asctime)s:%(levelname)s%(message)s')

def custom_text2dataframe(strg):
	if re.search(".",strg):
    	x= pattern.search(strg)
    	return(x.group(1),x.group(2),x.group(3),x.group(4),x.group(5),x.group(6),x.group(7),x.group(8),x.group(9),x.group(10))
	else:
    	return("ND","ND","ND","ND","ND","ND","ND","ND","ND","ND")


def removing_garbage(dataframe):
	df = dataframe.filter("ip1!='ND'")
	logging.info("Function removing_garbage succesfully executed")
	return df

def cast2Integer(dataframe,column_name):
	df = dataframe.withColumn(column_name,col(column_name).cast('Integer'))
	logging.info("Function cast2Integer succesfully executed")
	return df

def string2TimeStamp(dataframe,column_name):
	df=dataframe.withColumn(column_name,to_timestamp(col('timestamp'), time_pattern))
	logging.info("Function string2TimeStamp succesfully executed")
	return df

def analyticsLogic(dataframe,column_name):
	logging.info("Function analyticsLogic succesfully executed")
	pass



if __name__ == '__main__':
	environment = sys.argv[1]
	spark_lst = SparkSessionWrapper.SparkSession_initialize(environment)
	spark     = spark_lst[0]
	input_dir = spark_lst[1]
	output_dir= spark_lst[2]

	textrdd = spark.sparkContext.textFile(input_dir)

	pattern = re.compile("^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] (\S+) (\S+) (\S+) (\d{3}) (\d+) (\S+)")

	time_pattern = 'dd/MMM/yyyy:HH:mm:ss z'

	column_names = ['ip1','ip2','ip3','datetime','conn_type','level','link_type','response_code','port','link']


	


	data_frame = textrdd.map(lambda x:custom_text2dataframe(x))
	data_frame = removing_garbage(data_frame)
	data_frame = cast2Integer(data_frame,'port')
	data_frame = string2TimeStamp(data_frame,'datetime')
	
	spark.stop()
