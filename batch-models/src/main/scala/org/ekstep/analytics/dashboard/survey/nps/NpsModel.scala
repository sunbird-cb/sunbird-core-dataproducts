package org.ekstep.analytics.dashboard.survey.nps

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{coalesce, col, current_date, expr, from_unixtime, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._

object NpsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.survey.nps.NpsModel"
  implicit var debug: Boolean = false
  /**
   * Pre processing steps before running the algorithm. Few pre-process steps are
   * 1. Transforming input - Filter/Map etc.
   * 2. Join/fetch data from LP
   * 3. Join/Fetch data from Cassandra
   */
  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  /**
   * Method which runs the actual algorithm
   */
  override def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = events.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processUserReport(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  /**
   * Post processing on the algorithm output. Some of the post processing steps are
   * 1. Saving data to Cassandra
   * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
   * 3. Transform into a structure that can be input to another data product
   */
  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processUserReport(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config


    val cassandraData = npsTriggerC1DataFrame() //gives data from cassandra for users who have submitted the surveyform in last 3 months
    val druidData = npsTriggerC2DataFrame() // gives data from druid for users who have either completed 1 course or have more than 30 telemetry events
    val mongodbData = npsTriggerC3DataFrame() // gives the data from mongoDB for the users who have posted atleast 1 discussion

    val df = druidData.join(mongodbData, Seq("userid"), "inner").select(col("userid").alias("userid"))
    val filteredDF = df.except(cassandraData)

    show(filteredDF)
    //create an additional dataframe that has columns of user_feed table as we have to insert thes userIDS to user_feed table

    val additionalDF = filteredDF.withColumn("category", lit("NPS"))
      .withColumn("createdby", lit(null.asInstanceOf[String]))
      .withColumn("createdon", current_date())
      .withColumn("data", lit("{\"formId\":1694585805603}"))
      .withColumn("expireon", lit(null.asInstanceOf[String]))
      .withColumn("priority", lit(1))
      .withColumn("status", lit("unread"))
      .withColumn("updatedby", lit(null.asInstanceOf[String]))
      .withColumn("updatedon", lit(null.asInstanceOf[String]))


// write the dataframe to cassandra user_feed table

    additionalDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> conf.cassandraUserFeedKeyspace , "table" -> conf.cassandraUserFeedTable))
      .mode("append")
      .save()
  }
}
