from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as func
# import datetime
from datetime import date, datetime
from datetime import timedelta
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.master('local').appName('EODBatchLoad').getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

# function to calculate number of seconds from minutes
minutes = lambda i: i * 60

df = spark.read.parquet("dbfs:/output_dir/trade")
df.createOrReplaceTempView("trades")

# Record Unique Identified trade date, record type, symbol, event time, event sequence number.
##30-min Moving Average
df2 = spark.sql(
    "select trade_dt,symbol,event_tm, event_seq_no, trade_price , trade_size , exchange from trades where trade_dt = '2020-08-06' order by symbol, event_tm")
# df2.createOrReplaceTempView("tmp_trade_moving_avg")
df2 = df2.withColumn('event_tm_sec', df2.event_tm.cast('timestamp'))
windowSpec = Window.partitionBy("symbol").orderBy(func.col("event_tm_sec").cast('long')).rangeBetween(-minutes(30), 0)
mov_avg_df = df2.withColumn('RollingThirtyMinsAvg', func.avg("trade_price").over(windowSpec))
# mov_avg_df.write.saveAsTable("temp_trade_moving_avg")
mov_avg_df.createOrReplaceTempView("tmp_trade_moving_avg")

# latest trade price from previous day
current_date = datetime.strptime('2020-08-06', '%Y-%m-%d').date()
prev_date_strm = current_date - timedelta(days=1)

last_pr_df = spark.sql(
    f"select symbol,trade_price from  (select symbol,trade_price, DENSE_RANK() over (partition by exchange,symbol order by event_tm DESC) as rank from trades where trade_dt = '{prev_date_strm}' ) where rank=1")
last_pr_df.createOrReplaceTempView("last_pr_df")

## Populate latest trade price and latest 30 mins moving avg price for quote
dfq = spark.read.parquet("dbfs:/output_dir/quote")
dfq.createOrReplaceTempView("quotes")

quote_update = spark.sql(
    "select trade_dt , 'Q' as rec_type , symbol, event_tm , event_seq_no , exchange , bid_price , bid_size , ask_price , ask_size from quotes where trade_dt = '2020-08-06'")
quote_update.createOrReplaceTempView("tmp_quote_update")

latest_values = spark.sql(
    "select symbol as t_symbol , trade_price as last_trade_price ,RollingThirtyMinsAvg as last_avg_price from (select symbol , trade_price  ,RollingThirtyMinsAvg , DENSE_RANK() over (partition by symbol order by event_tm desc) as rank from tmp_trade_moving_avg where trade_dt='2020-08-06') where rank=1")

latestdf = quote_update.join(broadcast(latest_values), quote_update.symbol == latest_values.t_symbol, "left")
latestdf.createOrReplaceTempView("tmp_latestdf")

quote_final = spark.sql(
    "select trade_dt, event_tm, event_seq_no, exchange,bid_price, bid_size, ask_price, ask_size, last_trade_price, last_avg_price,bid_price - trade_price as bid_pr_mv, ask_price - trade_price as ask_pr_mv from tmp_latestdf left outer join last_pr_df on tmp_latestdf.symbol=last_pr_df.symbol")

quote_final.write.partitionBy("trade_dt").mode("overwrite").parquet("dbfs:/output_dir/etlquote")



