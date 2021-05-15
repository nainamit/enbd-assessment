# enbd-assessment

Reading Config file from resources.
  Extracting Kafka and Spark info from config file.
  Using spark info to create spark session.
  Reading stream from kafka using kafka info.

  
Streams
  1. Created Account Stream
  2. Created Loan Streams

Reading Data from streams.
   Extracting value from streams and parsing the Account and Loan Messages
   Added watermark for account and loan data.
   Joined the streams with watermark.
   Created 1 min window
   
   
Created  LoanAccount and AccountLoanStatistics 
The LoanAccount is a case class, responsible for handling the data from the kafka system. AccountLoanStatistics  is internal case class, that will be used to store and manipulate data about Account and Loan.   

  
I am  using mapGroupsWithState method of the KeyValueGroupedDataset class to actually implement the aggregation transformation.
These parameters are:
ID of your grouped computation. In our case, it will be AccountType
Iterator[V] is an iterator over the new values, coming from the batch for this particular ID.
GroupState[S] is an object, that gives you the state API.


When you will start this stream, the following computations will be executed per each micro-batch:
Read new data from the kafka
New records will be grouped by respective AccountType
If the state for provided AccountType exists, then get it and update the state
Else, just create a plain AccountLoanStatistics with corresponding id and empty details.
Update the statistics with new data
Return it to the stream

OUTPUT

+-----------+----------+-------+------------+
|AccountType|totalCount|amount |lastMinCount|
+-----------+----------+-------+------------+
|1          |2         |20000.0|2           |
|2          |2         |4000.0 |1           |
+-----------+----------+-------+------------+