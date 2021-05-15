package com.enbd.test.transactions
import java.nio.file.Files

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{ Dataset, SQLContext, SparkSession }
import com.enbd.test.Enbd_Development.consumer._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans.logical.MapGroups
import org.apache.spark.sql.execution.MapGroupsExec
import org.apache.spark.sql.execution.streaming.StateStoreReader
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.execution.streaming.state.FlatMapGroupsWithStateExecHelper.StateData
import com.typesafe.config.ConfigFactory

object AccountLoanStreamingApp extends App with Logging {

  
  val appName: String = "account-loan-state"
  val checkpointLocation: String = Files.createTempDirectory(appName).toString
  var query: StreamingQuery = _

  log.info(s"Running the process in temporary directory $checkpointLocation")
  val config = ConfigFactory.load("kafka_config.conf").getConfig("kafka-spark")
   val consumerConf = config.getConfig("consumer")
   val sparkConf = config.getConfig("spark")
   val topics = config.getString("topics").split(",")
   val accountTopic = topics(0)
   val loanTopic = topics(1)
  
  val spark = SparkSession.builder()
    // spark props
    .master(sparkConf.getString("master"))
    .appName(appName)
    .config("spark.driver.memory", sparkConf.getString("spark.driver.memory"))
    .config("spark.sql.shuffle.partitions", sparkConf.getString("spark.sql.shuffle.partitions"))
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  import spark.implicits._

  implicit val sqlCtx: SQLContext = spark.sqlContext

  val accountSchema: StructType = new StructType()
    .add("AccountId", DataTypes.LongType)
    .add("AccountType", DataTypes.IntegerType)

  val loanSchema: StructType = new StructType()
    .add("LoanId", DataTypes.LongType)
    .add("AccountId", DataTypes.LongType)
    .add("Amount", DataTypes.DoubleType)
    
    

  val account = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", consumerConf.getString("bootstrap.servers"))
    .option("startingOffsets", consumerConf.getString("auto.offset.reset"))
    .option("subscribe", accountTopic)
    .load().selectExpr("topic", "CAST(value AS STRING) as value", "timestamp").select(col("timestamp").cast("timestamp"), from_json(col("value"), accountSchema).as("values"))
    .selectExpr("values.AccountId", "values.AccountType", "timestamp")

  val loan = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", consumerConf.getString("bootstrap.servers"))
    .option("startingOffsets", consumerConf.getString("auto.offset.reset"))
    .option("subscribe", loanTopic)
    .load().selectExpr("topic", "CAST(value AS STRING) as value", "timestamp").select(col("timestamp").cast("timestamp"), from_json(col("value"), loanSchema).as("values"))
    .selectExpr("values.LoanId", "values.LoanId as LoanAccountId", "values.Amount", "timestamp as loanTimestamp")


  val accountWithWatermark = account.withWatermark("timestamp", "1 hours")
  val loanWithWatermark = loan.withWatermark("loanTimestamp", "2 hours")

  val joinStream = loanWithWatermark.join(
    accountWithWatermark,
    expr("""
    LoanAccountId = AccountId AND 
    loanTimestamp >= timestamp AND
    loanTimestamp <= timestamp + interval 60 seconds 
    
    """),
    joinType = "leftOuter").select("AccountId", "AccountType", "LoanId", "Amount", "timestamp").filter(col("AccountType").isNotNull)

  joinStream.printSchema()
  val windows = joinStream.groupBy(window($"timestamp", "1 minute", "1 minute"), $"AccountType")
  import scala.concurrent.duration._

  val pageVisitsTypedStream: Dataset[AccountLoan] = joinStream.as(AccountLoanEncoder)
  import scala.concurrent.duration._
  
  // Adding data in state and updating existing state
  val noTimeout = GroupStateTimeout.NoTimeout()
  val userStatisticsStream = pageVisitsTypedStream
    .groupByKey(_.AccountType)
    .mapGroupsWithState(noTimeout)(updateAccountLoanStatistics)

    
   // printing data with states as even we can write data also in different different target location 
  query = userStatisticsStream.writeStream
    .outputMode(OutputMode.Update())
    .option("checkpointLocation", checkpointLocation)
    .foreachBatch(printBatch _).trigger(Trigger.ProcessingTime(60.seconds))
    .start()

  processDataWithLock(query)
  
// Reading existing states
  StateDatasetProvider(spark, checkpointLocation, query).dataset.sort($"AccountType").show(false)

  query.awaitTermination()

 
  
  
  /*
   * This method is used to update and store the states
   */
  def updateAccountLoanStatistics(
    id:        Int,
    newEvents: Iterator[AccountLoan],
    oldState:  GroupState[AccountLoanStatistics]): AccountLoanStatistics = {

    var state: AccountLoanStatistics = if (oldState.exists) oldState.get else AccountLoanStatistics(id, 0, 0.0, 0)

    var cnt = 0
    for (event <- newEvents) {
      cnt = cnt + 1
      state = state.copy(totalCount = state.totalCount + 1, amount = state.amount + event.Amount, lastMinCount = cnt)
      oldState.update(state)
    }

    state
  }

  def printBatch(batchData: Dataset[AccountLoanStatistics], batchId: Long): Unit = {
    log.info(s"Started working with batch id $batchId")
    log.info(s"Successfully finished working with batch id $batchId, dataset size: ${batchData.count()}")
    batchData.show(false)
  }

  def processDataWithLock(query: StreamingQuery): Unit = {
    query.processAllAvailable()
    while (query.status.message != "Waiting for data to arrive") {
      log.info(s"Waiting for the query to finish processing, current status is ${query.status.message}")
      Thread.sleep(1)
    }
    log.info("Locking the thread for another 5 seconds for state operations cleanup")
    Thread.sleep(5000)
  }

}