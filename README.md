
**Equity Market Analysis Data Pipeline**

The goal of this project is to build an end-to-end data pipeline to ingest and process daily stock
market data from multiple stock exchanges. The pipeline should maintain the source data in a
structured format, organized by date. It also needs to produce analytical results that support
business analysis.

**Data Source**\
The source data used in this project is randomly generated stock exchange data.\
->Trades: records that indicate transactions of stock shares between broker-dealers.\
->Quotes: records of updates best bid/ask price for a stock symbol on a certain exchange.




**Step 1: Database Table Design**\
● Implement database tables to host the trade and quote data.\
● Since trade and quote data is added on a daily basis with high volume, the design needs
to ensure the data growth over time doesn’t impact query performance. Most of the
analytical queries operate on data within the same day.

**Step 2: Data Ingestion**\
● The source data comes in JSON or CSV files, which will be specified by file name
extension.\
● Each file is mixed with both trade and quote records. The code needs to identify them by
column rec_type: Trade is “T”, Quote is “Q”.\
● Exchanges are required to submit all their data before the market closes at 4 pm every
day. They might submit data in multiple batches. Your platform should pre-process the
data as soon as they arrive.\
● The ingestion process needs to drop records that do not follow the schema.

**Step 3: End of Day (EOD) Batch Load**\
● Loads all the progressively processed data for current day into daily tables for trade and
quote.\
● The Record Unique Identifier is the combination of columns: trade date, record type,
symbol, event time, event sequence number.\
● Exchanges may submit the records which are intended to correct errors in previous ones
with updated information. Such updated records will have the same Record Unique
Identifier, defined above. The process should discard the old record and load only the
latest one to the table.\
● Job should be scheduled to run at 5pm every day.

**Step 4: Analytical ETL Job**\
To help the business teams do better analysis, we need to calculate supplement information for
quote records. Spring Capital wants to see the trade activity behind each quote. As the platform
developer, you are required to provide these additional results for each quote event:\
● The latest trade price before the quote.\
● The latest 30 min moving average trade price before the quote.\
● The bid and ask price movement (difference) from the previous day's last trade price.
For example, given the last trade price of $30, bid price of $30.45 has a movement of
$0.45.\

**Step 5. Pipeline Orchestration:**\
● Design one or multiple workflows to execute the individual job.\
● Maintain a job status table to keep track of running progress of each workflow.\
● Support workflow/job rerun in case of failure while running the job.

**Software Used in Implementation**
Data Store :
MySql 5.7 on Azure cloud  
Azure Blob Storage

Compute: Databricks Apache Spark 3.1.1 Cluster on Azure Cloud
Job Schedule : Databricks Jon schedule on Azure Cloud
Programming Language: Python
Editor : Jupyter Notebook

**Tables Names:**
batch_control_tb = mantain the current and previous close of business day
![image](https://user-images.githubusercontent.com/75573079/127578491-7cbb41a4-ccfc-4c27-b887-253ec1d6c4a8.png)

**Job_status_tb= job status information**
![image](https://user-images.githubusercontent.com/75573079/127578783-f4d656ba-1b94-4115-82a5-11a317d67425.png)

**Azure Blob Storage for JSON , CSV and Parquet Files**
![image](https://user-images.githubusercontent.com/75573079/127578896-21de523e-0b93-4a60-bfd0-094adc51b4ef.png)

![image](https://user-images.githubusercontent.com/75573079/127578847-f87174f1-2108-41a9-b925-df4d4b62ec11.png)

**Spark Jobs on Databricks**
![image](https://user-images.githubusercontent.com/75573079/127578983-7308d1b4-e3cf-4f6f-9e8a-0efbb3fcb869.png)


**Databricks Spark Cluster**
![image](https://user-images.githubusercontent.com/75573079/127579030-e166846b-dd36-4a12-8df7-61de24e5fc88.png)




  




