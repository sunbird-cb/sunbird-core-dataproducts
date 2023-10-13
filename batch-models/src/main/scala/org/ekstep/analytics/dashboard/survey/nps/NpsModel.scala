package org.ekstep.analytics.dashboard.survey.nps

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{coalesce, col, current_date, expr, from_unixtime, lit}
import org.apache.spark.sql.types.{DateType, StringType}
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

    val druidData1 = npsTriggerC1DataFrame() //gives data from druid for users who have submitted the survey form in last 3 months
    val druidData2 = npsTriggerC2DataFrame() // gives data from druid for users who have either completed 1 course or have more than 30 telemetry events
    val mongodbData = npsTriggerC3DataFrame() // gives the data from mongoDB for the users who have posted atleast 1 discussion

    csvWrite(druidData1.coalesce(1), "/tmp/nps-test/druidData1/")
    csvWrite(druidData2.coalesce(1), "/tmp/nps-test/druidData2/")
    csvWrite(mongodbData.coalesce(1), "/tmp/nps-test/mongodbData/")

    val df = druidData2.union(mongodbData)
    val druidData1Count = druidData1.count()
    println(s"DataFrame Count for set of users who have submitted the form: $druidData1Count")
    val druidData2Count = druidData2.count()
    println(s"DataFrame Count for set of users who have completed a course or have had atleast 15 sessions: $druidData2Count")
    val mongodbDataCount = mongodbData.count()
    println(s"DataFrame Count for set of users who have posted atleast 1 discussion: $mongodbDataCount")
    val totalCount = df.count()
    println(s"DataFrame Count for users who are eligible: $totalCount")
    val filteredDF = df.except(druidData1)
    val filteredCount = filteredDF.count()
    println(s"DataFrame Count for set of users who are eligible and not filled form: $filteredCount")
   // check if the feed for these users is alreday there
    val cassandraDF = userFeedFromCassandraDataFrame()
    csvWrite(cassandraDF.coalesce(1), "/tmp/nps-test/cassandraDF/")

    val existingFeedCount = cassandraDF.count()
    println(s"DataFrame Count for users who have feed data: $existingFeedCount")
    val storeToCassandraDF = filteredDF.except(cassandraDF)
    val finalFeedCount = storeToCassandraDF.count()
    println(s"DataFrame Count for final set of users to create feed: $finalFeedCount")
 
    print("{\"dataValue\":\"yes\",\"actionData\":{\"formId\":"+conf.platformRatingSurveyId+"}}")
    //create an additional dataframe that has columns of user_feed table as we have to insert thes userIDS to user_feed table
 
    val additionalDF = storeToCassandraDF.withColumn("category", lit("NPS"))
      .withColumn("id", expr("uuid()").cast(StringType))
      .withColumn("createdby", lit("platform_rating"))
      .withColumn("createdon", current_date())
      .withColumn("action", lit("{\"dataValue\":\"yes\",\"actionData\":{\"formId\":"+conf.platformRatingSurveyId+"}}"))
      .withColumn("expireon", lit(null).cast(DateType))
      .withColumn("priority", lit(1))
      .withColumn("status", lit("unread"))
      .withColumn("updatedby", lit(null).cast(StringType))
      .withColumn("updatedon", lit(null).cast(DateType))
      .withColumn("version", lit("v1"))
 

    show(additionalDF)
    csvWrite(additionalDF.coalesce(1), "/tmp/nps-test/additionalDF/")
 
// write the dataframe to cassandra user_feed table
 
   //additionalDF.write
   //   .format("org.apache.spark.sql.cassandra")
   //   .options(Map("keyspace" -> conf.cassandraUserFeedKeyspace , "table" -> conf.cassandraUserFeedTable))
   //   .mode("append")
   //  .save()
  }

}
