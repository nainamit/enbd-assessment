package com.enbd.test.transactions

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.enbd.test.Enbd_Development.consumer._

case class StateDatasetProvider(spark: SparkSession, checkpointLocation: String, query: StreamingQuery) {

  private val context = spark.sqlContext

  import context.implicits._

  private val keySchema = new StructType().add(StructField("id", IntegerType))
  private val valueSchema = new StructType().add(StructField("groupState", AccountLoanStatisticsEncoder.schema))

  val storeConf = StateStoreConf(spark.sessionState.conf)
  private val hadoopConf = spark.sessionState.newHadoopConf()


  val stateStoreId = StateStoreId(checkpointLocation + "/state", operatorId = 0, partitionId = 0)
  val storeProviderId = StateStoreProviderId(stateStoreId, query.runId)

  val store: StateStore = StateStore.get(storeProviderId, keySchema, valueSchema, None, query.lastProgress.batchId, storeConf, hadoopConf)

  val dataset: Dataset[AccountLoanStatistics] = store.iterator().map { rowPair =>
    val statisticsEncoder = ExpressionEncoder[AccountLoanGroupState].resolveAndBind()
    statisticsEncoder.fromRow(rowPair.value).groupState
  }.toSeq.toDS()

}