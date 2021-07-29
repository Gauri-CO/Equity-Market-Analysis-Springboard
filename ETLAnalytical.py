
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as func
# import datetime
from datetime import date, datetime
from datetime import timedelta
from pyspark.sql.functions import broadcast
import mysql.connector
from mysql.connector import Error


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

try:
    spark = SparkSession.builder.master('local').appName('EODBatchLoad').getOrCreate()
    spark.conf.set("fs.azure.account.key.retailanalytics08.blob.core.windows.net",
               "Vk+/9Z9quDj3p1cY1AIaZ4GrCr+bNgU7JnknY9DpuWlF6o31jWo6wrOSU3rciJs4sxQ0+M8dItaTrhhCgGsJzQ==")
    outfile = "wasbs://data@retailanalytics08.blob.core.windows.net/output"

    con = get_db_connection()
    cursor = con.cursor()
    print("You're connected to database")
    cursor.execute("select curr_dt, prev_dt from batch_control_tb where curr_ind=1;")
    record = list(cursor.fetchone())
    current_date = record[0]
    previous_date = record[1]
    Job_Id = "ETLAnalyticalJob" + "_" + str(current_date)
    print("Batch is running for Job Id : {}".format(Job_Id))
    print("Batch is running for {}".format(current_date))
    print("Previous Batch Date {}".format(previous_date))

    # function to calculate number of seconds from minutes
    minutes = lambda i: i * 60

    df = spark.read.parquet("dbfs:/output_dir/trade")
    df.createOrReplaceTempView("trades")

    # Record Unique Identified trade date, record type, symbol, event time, event sequence number.
    ##30-min Moving Average

    df2 = spark.sql(f"select trade_dt,symbol,event_tm, event_seq_no, trade_price , trade_size , exchange from trades where trade_dt = '{current_date}' order by symbol, event_tm")
    # df2.createOrReplaceTempView("tmp_trade_moving_avg")
    df2 = df2.withColumn('event_tm_sec', df2.event_tm.cast('timestamp'))
    windowSpec = Window.partitionBy("symbol").orderBy(func.col("event_tm_sec").cast('long')).rangeBetween(-minutes(30), 0)
    mov_avg_df = df2.withColumn('RollingThirtyMinsAvg', func.avg("trade_price").over(windowSpec))
    # mov_avg_df.write.saveAsTable("temp_trade_moving_avg")
    mov_avg_df.createOrReplaceTempView("tmp_trade_moving_avg")

    # latest trade price from previous day
    # current_date = datetime.strptime(current_date,'%Y-%m-%d').date()
    # prev_date_strm = current_date - timedelta(days = 1)
    prev_date_strm = previous_date

    last_pr_df = spark.sql(f"select symbol,trade_price from  (select symbol,trade_price, DENSE_RANK() over (partition by exchange,symbol order by event_tm DESC) as rank from trades where trade_dt = '{prev_date_strm}' ) where rank=1")
    last_pr_df.createOrReplaceTempView("last_pr_df")

    ## Populate latest trade price and latest 30 mins moving avg price for quote
    dfq = spark.read.parquet("dbfs:/output_dir/quote")
    dfq.createOrReplaceTempView("quotes")

    quote_update = spark.sql(f"select trade_dt , 'Q' as rec_type , symbol, event_tm , event_seq_no , exchange , bid_price , bid_size , ask_price , ask_size from quotes where trade_dt = '{current_date}'")
    quote_update.createOrReplaceTempView("tmp_quote_update")

    latest_values = spark.sql(f"select symbol as t_symbol , trade_price as last_trade_price ,RollingThirtyMinsAvg as last_avg_price from (select symbol , trade_price  ,RollingThirtyMinsAvg , DENSE_RANK() over (partition by symbol order by event_tm desc) as rank from tmp_trade_moving_avg where trade_dt='{current_date}') where rank=1")

    latestdf = quote_update.join(broadcast(latest_values), quote_update.symbol == latest_values.t_symbol, "left")
    latestdf.createOrReplaceTempView("tmp_latestdf")

    quote_final = spark.sql("select trade_dt, event_tm, event_seq_no, exchange,bid_price, bid_size, ask_price, ask_size, last_trade_price, last_avg_price,bid_price - trade_price as bid_pr_mv, ask_price - trade_price as ask_pr_mv from tmp_latestdf left outer join last_pr_df on tmp_latestdf.symbol=last_pr_df.symbol")

    # quote_final.write.partitionBy("trade_dt").mode("overwrite").parquet("dbfs:/output_dir/etlquote")

    quote_final.write.partitionBy("trade_dt").mode("overwrite").parquet(outfile)

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



