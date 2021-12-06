import findspark
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
import os
import subprocess
import argparse
import numpy as np


def get_args():
	"""
	Parses Command Line Args
	"""
	parser = argparse.ArgumentParser(description='Some Basic Spark Job doing some stuff on IMDb data stored within HDFS.')
	parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/imdb', required=True, type=str)
	parser.add_argument('--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/imdb/ratings', required=True, type=str)
	parser.add_argument('--hdfs_target_format', help='HDFS target format, e.g. csv or parquet or...', required=True, type=str)
	
	return parser.parse_args()


def haversine(lat1, lon1, lat2, lon2):
	"""
	slightly modified version: of http://stackoverflow.com/a/29546836/2901002

	Calculate the great circle distance between two points
	on the earth (specified in decimal degrees or in radians)
	
	earth_radius is given as kilometer, for miles use earth_radius=3959

	All (lat, lon) coordinates must have numeric dtypes and be of equal length.

	"""
	to_radians=True
	earth_radius=6371
	try:
		if to_radians:
			lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])

		a = np.sin((lat2-lat1)/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2-lon1)/2.0)**2

		return str(earth_radius * 2 * np.arcsin(np.sqrt(a)))
	except:
		return '-1'
	
def timeslotOld(st):
	try:
		return str(st.hour // 6)
	except:
		return '-1'
		

def timeslot(st):
	slots = ['0-6 Uhr', '6-12 Uhr', '12-18 Uhr', '18-24 Uhr']
	slot = st.hour // 6
	slotname = slots[slot]
	try:
		return slot
	except:
		return '0'

udf_haversine = F.udf(haversine)
udf_timeslot = F.udf(timeslot)

# Parse Command Line Args
args = get_args()
print('Initiating Spark Session')
# Initialize Spark Context
sc = pyspark.SparkContext()
spark = SparkSession(sc)
print('Initiating Spark Session complete')

for year in [2015,2016,2017]:
	for month in range(1, 13):
		# data doesnt exists for December 2016
		if year == 2016 and month == 12:
			continue
		file = f'{args.hdfs_source_dir}/{year}{str(month).zfill(2)}/{year}{str(month).zfill(2)}-hubway-tripdata.csv'
		print(f'Found file {file}')
		
		#create csv schema
		mySchema = StructType([
			StructField("tripdur", IntegerType(), True),
			StructField("starttime", TimestampType(), True),
			StructField("stoptime", TimestampType(), True),
			StructField("ssid", IntegerType(), True),
			StructField("ssname", StringType(), True),
			StructField("sslat", DoubleType(), True),
			StructField("sslon", DoubleType(), True),
			StructField("esid", IntegerType(), True),
			StructField("esname", StringType(), True),
			StructField("eslat", DoubleType(), True),
			StructField("eslon", DoubleType(), True),
			StructField("bikeid", IntegerType(), True),
			StructField("usertype", StringType(), True),
			StructField("birthyear", IntegerType(), True),
			StructField("gender", IntegerType(), True),
		])
		
		df = spark.read.format('csv') \
			.option("header", True) \
			.option("delimiter", ",") \
			.option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
			.schema(mySchema) \
			.load(file)
		
		df.show()
		
		print('\tCalculating timeslot')
		# calculate timeslot
		df = df.withColumn('timeslot', udf_timeslot(col('starttime')).cast(IntegerType()))
		
		print('\tCalculating distance')
		#calculate distance
		df = df.withColumn('tripdist', udf_haversine(col('sslat'), col('sslon'), col('eslat'), col('eslon')).cast(DoubleType()))
				
		print('\tFilter unusable rows')
		# filter unusable columns
		df = df.filter(col('tripdur') > 0) \
				.filter(col('stoptime') > col('starttime')) \
				.filter(col('gender') != 0)
			
		df.show()
		
		print('\tDropping columns')
		#drop columns that are not needed 
		df = df.drop(col('sslat')) \
				.drop(col('sslon')) \
				.drop(col('eslat')) \
				.drop(col('eslon')) \
				.drop(col('starttime')) \
				.drop(col('stoptime')) \
				.drop(col('usertype')) \
				
		df.show()
		
		print(f'Write filtered csv file: {args.hdfs_target_dir}/{year}{str(month).zfill(2)}/data.csv')
		# write filtered csv to final data
		df.write.format('csv') \
				.mode('overwrite') \
				.save(f'{args.hdfs_target_dir}/{year}{str(month).zfill(2)}/data.csv')
		
		