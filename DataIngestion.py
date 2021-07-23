import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType, DateType, \
    TimestampType
from pyspark.sql import SparkSession
import json
from typing import List
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

common_event = StructType([ \
    StructField("trade_dt", DateType(), True), \
    StructField("rec_type", StringType(), True), \
    StructField("symbol", StringType(), True), \
    StructField("exchange", StringType(), True), \
    StructField("event_tm", TimestampType(), True), \
    StructField("event_seq_no", IntegerType(), True), \
    StructField("arrival_tm", TimestampType(), True), \
    StructField("trade_price", FloatType(), True), \
    StructField("trade_size", IntegerType(), True), \
    StructField("bid_price", FloatType(), True), \
    StructField("bid_size", IntegerType(), True), \
    StructField("ask_price", FloatType(), True), \
    StructField("ask_size", IntegerType(), True), \
    StructField("partition", StringType(), True) \
    ])


def parse_csv(line):
    record_type_pos = 2

    record = line.split(",")

    trade_dt = datetime.strptime(record[0], '%Y-%m-%d').date()
    event_tm = datetime.strptime(record[4], "%Y-%m-%d %H:%M:%S.%f")
    file_tm = datetime.strptime(record[1], "%Y-%m-%d %H:%M:%S.%f")
    event_seq_nb = int(record[5])
    trade_pr = float(record[7])
    trade_size = int(record[8])
    bid_pr = float(record[7])
    bid_size = int(record[8])

    try:
        # [logic to parse records]
        if record[record_type_pos] == "T":
            event = (trade_dt, record[2], record[3], record[6], event_tm, event_seq_nb, file_tm, trade_pr, trade_size,
                     None, None, None, None, "T")
            return event
        elif record[record_type_pos] == "Q":
            ask_pr = float(record[9])
            ask_size = int(record[10])
            event = (trade_dt, record[2], record[3], record[6], event_tm, event_seq_nb, file_tm, None,
                     None, bid_pr, bid_size, ask_pr, ask_size, "Q")
            return event
    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        event = (None, None, None, None, None, None, None, None, None, None, None, None, None, "B")
        return event


def parse_json(line):
    record = json.loads(line)

    record_type = record['event_type']
    trade_dt = datetime.strptime(record['trade_dt'], '%Y-%m-%d').date()
    event_tm = datetime.strptime(record['event_tm'], "%Y-%m-%d %H:%M:%S.%f")
    file_tm = datetime.strptime(record['file_tm'], "%Y-%m-%d %H:%M:%S.%f")
    event_seq_nb = int(record['event_seq_nb'])

    try:
        # [logic to parse records]
        if record_type == "T":  # [Get the applicable field values from json]
            if record['trade_dt'] != "" and record['event_type'] != "" and record['symbol'] != "" and record[
                'event_tm'] != "" and record['event_seq_nb'] != "":
                eventj = (trade_dt, record['event_type'], record['symbol'], record['exchange'],
                          event_tm, event_seq_nb, file_tm, record['price'],
                          record['size'], None, None, None, None, "T")
            else:
                eventj = (None, None, None, None, None, None, None, None, None, None, None, None, None, "B")

            return eventj

        elif record_type == "Q":
            # [Get the applicable field values from json]
            if record['trade_dt'] != "" and record['event_type'] != "" and record['symbol'] != "" and record[
                'event_tm'] != "" and record['event_seq_nb'] != "":
                eventj = (trade_dt, record['event_type'], record['symbol'], record['exchange'],
                          event_tm, event_seq_nb, file_tm, None,
                          None, record['bid_pr'], record['bid_size'], record['ask_pr'],
                          record['ask_size'], "Q")
            else:
                eventj = (None, None, None, None, None, None, None, None, None, None, None, None, None, "B")

            return eventj

    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        return (None, None, None, None, None, None, None, None, None, None, None, None, None, "B")


if __name__ == "__main__":
    spark.conf.set("fs.azure.account.key.retailanalytics08.blob.core.windows.net",
                   "Vk+/9Z9quDj3p1cY1AIaZ4GrCr+bNgU7JnknY9DpuWlF6o31jWo6wrOSU3rciJs4sxQ0+M8dItaTrhhCgGsJzQ==")

    filenamecsv = "wasbs://data@retailanalytics08.blob.core.windows.net/csv"
    # filenamejson = "C://Users/gauri/PycharmProjects/GuidedCapstoneProject/capstonedocs/data/json/part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.txt"
    filenamejson = "wasbs://data@retailanalytics08.blob.core.windows.net/json"
    # output_dir="wasbs://output@retailanalytics08.blob.core.windows.net"
    spark = create_session()

    raw = spark.sparkContext.textFile(filenamecsv)
    parsed = raw.map(lambda line: parse_csv(line))
    data = spark.createDataFrame(parsed, common_event)
    data.show()
    data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

    rawj = spark.sparkContext.textFile(filenamejson)
    parsedj = rawj.map(lambda line: parse_json(line))
    dataj = spark.createDataFrame(parsedj, common_event)
    dataj.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

    print(data.count())
    print(dataj.count())



