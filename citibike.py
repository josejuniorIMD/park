# Copyright 2022 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https: // www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, year, current_date, count, trim
from pyspark.sql.types import BooleanType

if len(sys.argv) == 1:
    print("Please provide a dataset name.")

dataset = sys.argv[1]
table = "bigquery-public-data:new_york_citibike.citibike_trips"

spark = SparkSession.builder \
    .appName("pyspark-example") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
    .getOrCreate()

df = spark.read.format("bigquery").load(table)

# Calculate the average trip duration and display the 10 longest trips.
avg_trip_duration = df.agg(avg("tripduration").alias("avg_tripduration")).collect()[0]["avg_tripduration"]
print(f"A duração média das viagens é de {avg_trip_duration:.3f}seg")

df_top_longest_trips = df.orderBy("tripduration", ascending=False) \
                         .limit(10) \
                         .select("tripduration")
df_top_longest_trips.show()

# Calculate the average trip duration and display the 10 shortest trips (excluding trips with no duration).
avg_trip_duration_filtered = df.filter(col("tripduration").isNotNull()) \
                               .agg(avg("tripduration").alias("avg_tripduration")) \
                               .collect()[0]["avg_tripduration"]
print(f"A duração média da viagens é {avg_trip_duration_filtered:.3f}s")

df_top_shortest_trips = df.filter(col("tripduration").isNotNull()) \
                          .orderBy("tripduration", ascending=True) \
                          .limit(10) \
                          .select("tripduration")
df_top_shortest_trips.show()

# Calculate the average trip duration for each pair of stations, order them, and display the 10 longest.
df_avg_duration_by_station = df.groupBy("start_station_id", "end_station_id") \
                               .agg(avg("tripduration").alias("avg_duration")) \
                               .orderBy("avg_duration", ascending=False) \
                               .limit(10) \
                               .select("start_station_id", "end_station_id", "avg_duration")
df_avg_duration_by_station.show()

# Calculate the total number of trips per bike and display the 10 most used bikes.
df_total_trips_per_bike = df.filter(col("bikeid").isNotNull()) \
                            .groupBy("bikeid") \
                            .agg(count("*").alias("total_trips")) \
                            .orderBy("total_trips", ascending=False) \
                            .limit(10) \
                            .select("bikeid", "total_trips")
df_total_trips_per_bike.show()

# Calculate the average age of customers.
avg_age_of_customers = df.filter(col("birth_year").isNotNull()) \
                         .withColumn("age", year(current_date()) - col("birth_year")) \
                         .agg(avg("age").alias("avg_age")) \
                         .collect()[0]["avg_age"]
print(f"A média de idade dos clientes é: {avg_age_of_customers:.2f} anos de idade")

# Calculate the gender distribution.
df_gender_dist = df.filter(col("gender").isNotNull() & (trim(col("gender")) != "")) \
                   .groupBy("gender") \
                   .count()
df_gender_dist.show()

spark.stop()

