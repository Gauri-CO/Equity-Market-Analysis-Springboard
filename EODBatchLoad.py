
from pyspark.sql import SparkSession
import mysql.connector
from mysql.connector import Error

spark = SparkSession.builder.master('local').appName('EODBatchLoad').getOrCreate()

def get_db_connection():
    try:
        connection = mysql.connector.connect(host='batchdb.mysql.database.azure.com',
                                             database='batchstartdb',
                                             user='batchuser@batchdb',
                                             password='August@2021')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            return connection

    except Error as e:
        print("Error while connecting to MySQL", e)


def applyLatestTrade(trade):
  # Record Unique Identifier : trade date, record type,symbol, event time, event sequence number
    trade.createOrReplaceTempView("tc")
    trade_corrected = spark.sql("select trade_dt,rec_type,symbol,exchange,event_tm,event_seq_no,arrival_tm,trade_price,trade_size from tc where (trade_dt,rec_type,symbol,event_tm,event_seq_no,arrival_tm) in ( select trade_dt,rec_type,symbol,event_tm,event_seq_no,max(arrival_tm) from tc group by trade_dt,rec_type,symbol,event_tm,event_seq_no)")
    return trade_corrected
  
def applyLatestQuote(quote):
  # Record Unique Identifier : trade date, record type,symbol, event time, event sequence number
    quote.createOrReplaceTempView("qc")
    quote_corrected = spark.sql("select trade_dt,rec_type,symbol,exchange,event_tm,event_seq_no,arrival_tm,bid_price,bid_size,ask_price,ask_size from qc where (trade_dt,rec_type,symbol,event_tm,event_seq_no,arrival_tm) in ( select trade_dt,rec_type,symbol,event_tm,event_seq_no,max(arrival_tm) from qc group by trade_dt,rec_type,symbol,event_tm,event_seq_no)")
    return quote_corrected
  
if __name__ == "__main__":
  try:
      con = get_db_connection()
      cursor = con.cursor()
      print("You're connected to database")
      cursor.execute("select curr_dt, prev_dt from batch_control_tb where curr_ind=1;")
      record = list(cursor.fetchone())
      current_date = record[0]
      previous_date = record[1]
      Job_Id = "EODBatchLoadJob" + "_" + str(current_date)
      print("Batch is running for Job Id : {}".format(Job_Id))
      print("Batch is running for {}".format(current_date))
      print("Previous Batch Date {}".format(previous_date))
      trade_common = spark.read.parquet('dbfs:/output_dir/partition=T')
      quote_common = spark.read.parquet('dbfs:/output_dir/partition=Q')
      #trade_common.printSchema()
      trade = trade_common[["trade_dt","rec_type","symbol","exchange","event_tm","event_seq_no","arrival_tm","trade_price","trade_size"]]
      quote = quote_common[["trade_dt","rec_type","symbol","exchange","event_tm","event_seq_no",\
                        "arrival_tm","bid_price","bid_size","ask_price","ask_size"]]
      trade_corrected = applyLatestTrade(trade)
      quote_corrected = applyLatestQuote(quote)
      quote_corrected.write.partitionBy("trade_dt").mode("overwrite").parquet("dbfs:/output_dir/quote")
      trade_corrected.write.partitionBy("trade_dt").mode("overwrite").parquet("dbfs:/output_dir/trade")


#trade_corrected.write.parquet("dbfs:/output_dir/trade/trade_dt={}".format(trade_date))

      args = [Job_Id, 'SUCCESS']
      result_args = cursor.callproc('update_job_status', args)

  except Error as e:
        print("Error while connecting to MySQL", e)
        args = [Job_Id, 'FAILED']
        result_args = cursor.callproc('update_job_status', args)

  finally:
        if con.is_connected():
            cursor.close()
            con.close()
            print("MySQL connection is closed")


