package org.ekstep.analytics.dashboard.report.ratingReconciler

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.StorageUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate, StorageConfig}

object RatingReconcilerModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.user.RatingReconcilerModel"
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
    processRatingData(timestamp, config)
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

  def processRatingData(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // get user roles data
    var ratingDf = courseRatingTableDataFrame()
    import org.apache.spark.sql.functions._

    ratingDf = ratingDf.select(
      col("activityid"),
      col("activitytype"),
      col("rating"),
      col("userid")
    )

    val aggDF = ratingDf.groupBy("activityid","activitytype")
      .agg(
        sum("rating").alias("sum_of_total_ratings"),
        count("userid").alias("total_number_of_ratings"),
        sum(when(col("rating") === 1, 1).otherwise(0)).alias("totalcount1stars"),
        sum(when(col("rating") === 2, 1).otherwise(0)).alias("totalcount2stars"),
        sum(when(col("rating") === 3, 1).otherwise(0)).alias("totalcount3stars"),
        sum(when(col("rating") === 4, 1).otherwise(0)).alias("totalcount4stars"),
        sum(when(col("rating") === 5, 1).otherwise(0)).alias("totalcount5stars")
       )
    show(aggDF)

    aggDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "rating", "keyspace" -> "sunbird"))
      .mode("append")
      .save()

  //  closeRedisConnect()
  }
}
