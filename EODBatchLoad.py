from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('EODBatchLoad').getOrCreate()


def applyLatestTrade(trade):
    # Record Unique Identifier : trade date, record type,symbol, event time, event sequence number
    trade.createOrReplaceTempView("tc")
    trade_corrected = spark.sql(
        "select trade_dt,rec_type,symbol,exchange,event_tm,event_seq_no,arrival_tm,trade_price,trade_size from tc where (trade_dt,rec_type,symbol,event_tm,event_seq_no,arrival_tm) in ( select trade_dt,rec_type,symbol,event_tm,event_seq_no,max(arrival_tm) from tc group by trade_dt,rec_type,symbol,event_tm,event_seq_no)")
    return trade_corrected


def applyLatestQuote(quote):
    # Record Unique Identifier : trade date, record type,symbol, event time, event sequence number
    quote.createOrReplaceTempView("qc")
    quote_corrected = spark.sql(
        "select trade_dt,rec_type,symbol,exchange,event_tm,event_seq_no,arrival_tm,bid_price,bid_size,ask_price,ask_size from qc where (trade_dt,rec_type,symbol,event_tm,event_seq_no,arrival_tm) in ( select trade_dt,rec_type,symbol,event_tm,event_seq_no,max(arrival_tm) from qc group by trade_dt,rec_type,symbol,event_tm,event_seq_no)")
    return quote_corrected


trade_common = spark.read.parquet('dbfs:/output_dir/partition=T')
quote_common = spark.read.parquet('dbfs:/output_dir/partition=Q')
# trade_common.printSchema()
trade = trade_common[
    ["trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_no", "arrival_tm", "trade_price",
     "trade_size"]]
quote = quote_common[
    ["trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_no", "arrival_tm", "bid_price", "bid_size",
     "ask_price", "ask_size"]]
trade_corrected = applyLatestTrade(trade)
quote_corrected = applyLatestQuote(quote)
quote_corrected.write.partitionBy("trade_dt").mode("overwrite").parquet("dbfs:/output_dir/quote")
trade_corrected.write.partitionBy("trade_dt").mode("overwrite").parquet("dbfs:/output_dir/trade")

# trade_corrected.write.parquet("dbfs:/output_dir/trade/trade_dt={}".format(trade_date))


