package org.ekstep.analytics.dashboard.report.leaderboard

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework._

import java.io.Serializable


object LeaderboardMetricsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.leaderboard.LeaderboardMetricsModel"
  override def name() = "LeaderboardMetricsModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // obtain and save user org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
      allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true)
    var allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)

    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")

    val userCompletionDF = allCourseProgramCompletionWithDetailsDF
      .where(expr("category='Course' AND courseStatus IN ('Live', 'Retired') AND dbCompletionStatus=2"))
      .groupBy("userID")
      .agg(
        expr("COUNT(courseID)").alias("userCompletedCount"),
        expr("SUM(courseDuration)").alias("userCompletedDuration")
      )

    val userOrgCompletionDF = userOrgDF.join(userCompletionDF, Seq("userID"), "left")
      .na.fill(0, Seq("userCompletedCount"))
      .na.fill(0.0, Seq("userCompletedDuration"))

    validate({userOrgDF.count()}, {userOrgCompletionDF.count()}, "userOrgDF.count() should be equal to userOrgCompletionDF.count()")

    val loggedInMobileUserDF = loggedInMobileUserDataFrame()
    val loggedInWebUserDF = loggedInWebUserDataFrame()
    val actualTimeSpentLearningDF = actualTimeSpentLearningDataFrame()

    val leaderboardDF = userOrgCompletionDF
      .join(loggedInMobileUserDF, Seq("userID"), "left")
      .join(loggedInWebUserDF, Seq("userID"), "left")
      .join(actualTimeSpentLearningDF, Seq("userID"), "left")
      .na.fill(value = false, Seq("userLoginFromMobile", "userLoginFromWeb"))
      .na.fill(0.0, Seq("userActualTimeSpentLearning"))
      .select(
        "userID", "userStatus", "firstName", "lastName", "userOrgID", "userOrgName", "userOrgStatus",
        "userLoginFromMobile", "userLoginFromWeb", "userActualTimeSpentLearning",
        "userVerified", "userCompletedCount", "userCompletedDuration"
      )

    validate({userOrgDF.count()}, {leaderboardDF.count()}, "userOrgDF.count() should be equal to leaderboardDF.count()")

    csvWrite(leaderboardDF.coalesce(1), "/tmp/user-leaderboard-data/")

    Redis.closeRedisConnect()

  }

}