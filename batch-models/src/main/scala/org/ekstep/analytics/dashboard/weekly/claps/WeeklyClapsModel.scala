package org.ekstep.analytics.dashboard.weekly.claps

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

object WeeklyClapsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.weekly.claps.WeeklyClapsModel"

  override def name() = "WeeklyClapsModel"

  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = events.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processWeeklyClaps(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processWeeklyClaps(l: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // get weekStart, weekEnd and dataTillDate(previous day) from today's date
    val (weekStart, weekEnd, weekEndTime, dataTillDate) = getThisWeekDates()
//    val weekStart = ""     //for manual testing
//    val weekEndTime = ""

    //get existing weekly-claps data
    var df = learnerStatsDataFrame()
    // get platform engagement data from summary-events druid datasource
    val platformEngagementDF = usersPlatformEngagementDataframe(weekStart, weekEndTime)

    df = df.join(platformEngagementDF, Seq("userid"), "full")

    df = df.withColumn("w4", map(
      lit("timespent"), when(col("platformEngagementTime").isNull, 0).otherwise(col("platformEngagementTime")),
      lit("numberOfSessions"), when(col("sessionCount").isNull, 0).otherwise(col("sessionCount"))
    ))

    val condition = col("w4")("timespent") >= conf.cutoffTime && !col("claps_updated_this_week")

    if(dataTillDate.equals(weekEnd) && !dataTillDate.equals(df.select(col("last_updated_on")))) {
      JobLogger.log("Started weekend updates")
      df = df.select(
        col("w2").alias("w1"),
        col("w3").alias("w2"),
        col("w4").alias("w3"),
        col("w4"),
        col("total_claps"),
        col("userid"),
        col("platformEngagementTime"),
        col("sessionCount"),
        col("claps_updated_this_week")
      )
      df = df.withColumn("total_claps", when(col("w4")("timespent") < conf.cutoffTime, 0).otherwise(col("total_claps")))
        .withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps")))
        .withColumn("last_updated_on", lit(dataTillDate))
        .withColumn("claps_updated_this_week", lit(false))
        .withColumn("w4", map(lit("timespent"), lit(0.0), lit("numberOfSessions"), lit(0)))

      JobLogger.log("Completed weekend updates")

    } else {
      df = df.withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps")))
        .withColumn("claps_updated_this_week", when(condition, lit(true)).otherwise(col("claps_updated_this_week")))
    }

    df = df.withColumn("total_claps", when(col("total_claps").isNull, 0).otherwise(col("total_claps")))
      .withColumn("claps_updated_this_week", when(col("claps_updated_this_week").isNull, false).otherwise(col("claps_updated_this_week")))

    df = df.drop("platformEngagementTime","sessionCount")

    writeToCassandra(df, conf.cassandraUserKeyspace, conf.cassandraLearnerStatsTable)
  }
}