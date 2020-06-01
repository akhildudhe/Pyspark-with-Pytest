from . import LogParser
import pytest
import pandas as pd

@pytest.mark.usefixtures("spark_session")
class TestApplication:

	def custom_text2dataframe(self,spark_session):
		input_text=[" ",\
		"109.169.248.247 - - [12/Dec/2015:18:25:11 +0100] POST /administrator/index.php HTTP/1.1 200 4494 http://almhuette-raith.at/administrator/ Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0 -"\
		]

		expected_output = spark_session.createDataFrame(\
        [('ND', 'ND','ND','ND','ND','ND','ND','ND','ND','ND'),\
         ('109.169.248.247', '-','-','12/Dec/2015:18:25:11 +0100','POST','/administrator/index.php','HTTP/1.1','200','4494','http://almhuette-raith.at/administrator/')],\
        ['ip1','ip2','ip3','timestamp','conn_type','level','link_type','response_code','port','link'])
    	

		input_file = input_text.map(lambda x:LogParser.custom_text2dataframe(x))

    	pd.testing.assert_frame_equal(expected_output,real_output,check_like=True)


	def removing_garbage(self,spark_session):

	def cast2Integer(self,spark_session):

	def string2TimeStamp(self,spark_session):

	def analyticsLogic(self,spark_session):