package org.ekstep.analytics.dashboard.report.leaderboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework._


object LeaderboardMetricsModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.leaderboard.LeaderboardMetricsModel"
  override def name() = "LeaderboardMetricsModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

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

    csvWrite(leaderboardDF.coalesce(1), s"${conf.localReportDir}/user-leaderboard-data/")

    Redis.closeRedisConnect()

  }

}