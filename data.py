
import rasterio
from rasterio.plot import show
from rasterio.io import MemoryFile
import time
from datetime import datetime
import numpy as np
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, IntegerType,StructType,FloatType,StructField
import math

def generate_paths():
    const = "s3://elevation-tiles-prod/geotiff/10/"
    paths = []
    for x in range(485,640):
        for y in range(220,401):
            paths.append(f"{const}{x}/{y}.tif")
    return paths

def get_geo_elevation_array(byte):
    with MemoryFile(byte) as memfile:
        with memfile.open() as dt:
            data_arr = dt.read()
            return data_arr

def get_geo_bounds(byte):
    with MemoryFile(byte) as memfile:
        with memfile.open() as dt:
            return np.array(dt.bounds)

def get_mean_value(rdd_array):
    val = rdd_array[0]
    return np.mean(val)

def to_csv_line(data):
  return ';'.join(str(d) for d in data)


''' spark setup ''' 
spark = SparkSession.builder.appName("TerrainTiles").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

print("********START********")
start = time.time()

paths = generate_paths()
l = len(paths)
inc = 2000
whole = math.ceil(l/inc)

for i in range(whole):
    print(f"{i}/{whole}")
    files = paths[i*inc:(i+1)*inc]
    # put in rdd files from paths
    rdd = sc.binaryFiles(",".join(files))
    # convert every file to bytes
    rdd_bytes = rdd.map(lambda x: bytes(x[1]))
    # get raw elevation values
    rdd_array = rdd_bytes.map(lambda x: get_geo_elevation_array(x))
    # get metadata of each tiles
    rdd_bounds = rdd_bytes.map(lambda x: get_geo_bounds(x))
    # calculate mean elevation value of each area
    rdd_mean = rdd_array.map(get_mean_value)
    # zip meta with values and prepare to save
    zipped = rdd_bounds.zip(rdd_mean)
    zipped = zipped.map(lambda x: (f"[{x[0][0]},{x[0][1]},{x[0][2]},{x[0][3]}]",x[1]))
    lines = zipped.map(to_csv_line)

    # save rdd as csv
    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y %H:%M:%S")

    s3_bucket = 's3://YOUR_BUCKET_NAME'
    lines.saveAsTextFile(f'{s3_bucket}/{dt_string}.csv')

end = time.time()
print("time:")
print(end-start)