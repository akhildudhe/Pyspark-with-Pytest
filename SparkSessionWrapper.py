from pyspark.sql import SparkSession
import configparser

def SparkSession_initialize(environment):

	config = configparser.ConfigParser()
	config.read('application.properties')

	execution_mode = config.get(environment,'exection.mode')
	appname        = config.get(environment,'appname')
	executormemory = config.get(environment,'executormemory')
	executorcores  = config.get(environment,'executorcores')
	deploymode     = config.get(environment,'deploymode')
	hivemetastore  = config.get(environment,'hivemetastore')
	warehousedir   = config.get(environment,'warehousedir')


	inputdir       = config.get(environment,'input.base.dir')
	outputdir      = config.get(environment,'output.base.dir')
 
	spark = SparkSession.builder.master(execution_mode).appName(appname).getOrCreate()

	return [spark,inputdir,outputdir]
