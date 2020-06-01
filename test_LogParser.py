from . import LogParser
import pytest
import pandas as pd

@pytest.mark.usefixtures("spark_session")
class TestApplication:

	def test_custom_text2dataframe(self,spark_session):
		input_text=[" ",\
		"109.169.248.247 - - [12/Dec/2015:18:25:11 +0100] POST /administrator/index.php HTTP/1.1 200 4494 http://almhuette-raith.at/administrator/ Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0 -"\
		]

		expected_output = spark_session.createDataFrame(\
        [('ND', 'ND','ND','ND','ND','ND','ND','ND','ND','ND'),\
         ('109.169.248.247', '-','-','12/Dec/2015:18:25:11 +0100','POST','/administrator/index.php','HTTP/1.1','200','4494','http://almhuette-raith.at/administrator/')],\
        ['ip1','ip2','ip3','timestamp','conn_type','level','link_type','response_code','port','link'])
    	

		real_output = input_text.map(lambda x:LogParser.custom_text2dataframe(x))

    	pd.testing.assert_frame_equal(expected_output,real_output,check_like=True)


	def test_removing_garbage(self,spark_session):

		input_dataframe = spark_session.createDataFrame(\
        [('ND', 'ND','ND','ND','ND','ND','ND','ND','ND','ND'),\
         ('109.169.248.247', '-','-','12/Dec/2015:18:25:11 +0100','POST','/administrator/index.php','HTTP/1.1','200','4494','http://almhuette-raith.at/administrator/')],\
        ['ip1','ip2','ip3','timestamp','conn_type','level','link_type','response_code','port','link'])
		
		expected_output = spark_session.createDataFrame(\
        [('109.169.248.247', '-','-','12/Dec/2015:18:25:11 +0100','POST','/administrator/index.php','HTTP/1.1','200','4494','http://almhuette-raith.at/administrator/')],\
        ['ip1','ip2','ip3','timestamp','conn_type','level','link_type','response_code','port','link'])

        real_output = LogParser.removing_garbage(input_dataframe)

        pd.testing.assert_frame_equal(expected_output,real_output,check_like=True)

	def test_cast2Integer(self,spark_session):

		shme= StructType([StructField('name',StringType()),StructField('department',StringType()),StructField('sal',IntegerType())])
		expected_output = spark_session.createDataFrame([('Don','etc',78),('joe','mech',58)],['name','department','sal'],shme)

		input_dataframe = spark_session.createDataFrame([('Don','etc','78'),('joe','mech','58')],['name','department','sal'])

		real_output = LogParser.cast2Integer(input_dataframe,'sal')
		pd.testing.assert_frame_equal(expected_output,real_output,check_like=True)
		
		# shme = StructType([StructField('ip1',StringType()),\
		#  StructField('ip2',StringType()),\
		#  StructField('ip3',StringType()),\
		#  StructField('timestamp',StringType()),\
		#  StructField('conn_type',StringType()),\
		#  StructField('level',StringType()),\
		#  StructField('link_type',StringType()),\
		#  StructField('response_code',StringType()),\
		#  StructField('port',IntegerType()),\
		#  StructField('link',StringType())])


		# expected_output = spark_session.createDataFrame(\
  #       [('109.169.248.247', '-','-','12/Dec/2015:18:25:11 +0100','POST','/administrator/index.php','HTTP/1.1','200',4494,'http://almhuette-raith.at/administrator/')],\
  #       ['ip1','ip2','ip3','timestamp','conn_type','level','link_type','response_code','port','link'],shme)

		# input_dataframe = spark_session.createDataFrame(\
  #       [('109.169.248.247', '-','-','12/Dec/2015:18:25:11 +0100','POST','/administrator/index.php','HTTP/1.1','200','4494','http://almhuette-raith.at/administrator/')],\
  #       ['ip1','ip2','ip3','timestamp','conn_type','level','link_type','response_code','port','link'])


		# real_output = spark_session.cast2Integer(input_dataframe,'port')
		# pd.testing.assert_frame_equal(expected_output,real_output,check_like=True)


	def test_string2TimeStamp(self,spark_session):


		expected_output = spark_session.createDataFrame([('12/12/2015 18:31:25 +0100'),('12/01/2016 20:31:25 +0500')],['timestamp'])

		input_dataframe = spark_session.createDataFrame([('12/Dec/2015:18:31:25 +0100'),('12/Jan/2016 20:31:25 +0500')],['timestamp'])

		real_output = LogParser.string2TimeStamp(input_dataframe,'timestamp')
		
		pd.testing.assert_frame_equal(expected_output,real_output,check_like=True)
		


	def test_analyticsLogic(self,spark_session):