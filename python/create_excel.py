import findspark
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
import os
import subprocess
import argparse
from pandas import ExcelWriter
import pandas


def get_args():
	"""
	Parses Command Line Args
	"""
	parser = argparse.ArgumentParser(description='Some Basic Spark Job doing some stuff on IMDb data stored within HDFS.')
	parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/imdb', required=True, type=str)
	parser.add_argument('--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/imdb/ratings', required=True, type=str)
	return parser.parse_args()

# Parse Command Line Args
args = get_args()
print('Initiating Spark Session')
# Initialize Spark Context
sc = pyspark.SparkContext()
spark = SparkSession(sc)
print('Initiating Spark Session complete')
excel_file = f'/home/airflow/hubway_output/output.xlsx'
print(f'Excel-File:{excel_file}')

dataframes = {}

for year in [2015,2016,2017]:
	for month in range(1, 13):
		# data doesnt exists for December 2016
		if year == 2016 and month == 12:
			continue
		file = f'{args.hdfs_source_dir}/{year}{str(month).zfill(2)}/data.csv'
		print(f'Found file {file}')
		
		#create csv schema
		mySchema = StructType([
			StructField("tripdur", IntegerType(), True),
			StructField("ssid", IntegerType(), True),
			StructField("ssname", StringType(), True),
			StructField("esid", IntegerType(), True),
			StructField("esname", StringType(), True),
			StructField("bikeid", IntegerType(), True),
			StructField("birthyear", IntegerType(), True),
			StructField("gender", IntegerType(), True),
			StructField("timeslot", IntegerType(), True),
			StructField("tripdist", IntegerType(), True),
		])
		
		df = spark.read.format('csv') \
			.option("header", True) \
			.option("delimiter", ",") \
			.option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
			.schema(mySchema) \
			.load(file)
			
		
		# calculate share by timeslot
		total = df.count()
		df1 = df.groupBy("timeslot") \
				.count() \
				.withColumn('sharePerc', (col('count') / total) *100) \
				.orderBy('timeslot') \
				
		# calculate most used bikes
		df2 = df.groupBy('bikeid') \
				.count() \
				.withColumn('sharePerc', (col('count') / total) *100) \
				.orderBy(desc('sharePerc')) \
		
		# calculate most used start stations
		df3 = df.groupBy('ssid', 'ssname') \
				.count() \
				.withColumn('sharePerc', (col('count') / total) *100) \
				.orderBy(desc('sharePerc')) \
				
		# calculate most used end stations
		df4 = df.groupBy('esid', 'esname') \
				.count() \
				.withColumn('sharePerc', (col('count') / total) *100) \
				.orderBy(desc('sharePerc')) \
				
		# calculate share by gender
		df5 = df.groupBy('gender') \
				.count() \
				.withColumn('sharePerc', (col('count') / total) *100) \
				.orderBy(desc('gender')) \
				
		# calculate share by age
		df6 = df.groupBy('birthyear') \
				.count() \
				.withColumn('sharePerc', (col('count') / total) *100) \
				.orderBy(desc('birthyear')) \
				
		# calculate avg distance
		df7 = df.agg(avg(col("tripdist")))
		
		# calculate avg duration
		df8 = df.agg(avg(col("tripdur")))
		
		dataframes[f'{year}{str(month).zfill(2)}'] = [df7, df8, df1, df2, df3, df4, df5, df6]
		
	with ExcelWriter(excel_file, engine='xlsxwriter') as writer:
		for slide, dfs in dataframes.items():
			dfs[0].toPandas().to_excel(writer, slide, startrow=1, startcol=0)
			dfs[1].toPandas().to_excel(writer, slide, startrow=5, startcol=0)
			dfs[2].toPandas().to_excel(writer, slide, startrow=9, startcol=0)
			dfs[3].limit(10).toPandas().to_excel(writer, slide, startrow=18, startcol=0)
			dfs[4].limit(10).toPandas().to_excel(writer, slide, startrow=30, startcol=0)
			dfs[5].limit(10).toPandas().to_excel(writer, slide, startrow=46, startcol=0)
			dfs[6].toPandas().to_excel(writer, slide, startrow=58, startcol=0)
			dfs[7].toPandas().to_excel(writer, slide, startrow=65, startcol=0)
			
			#writer.save()
		
		
		