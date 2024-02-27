package org.ekstep.analytics.dashboard.report.rozgar

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round, to_date}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext


object RozgarReportModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.rozgar.RozgarReportModel"
  override def name() = "RozgarReportModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()
    val reportPath = s"${conf.localReportDir}/${conf.userEnrolmentReportPath}/${today}/"

    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // Get course data first
    val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"))

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()

    val userRatingDF = userCourseRatingDataframe()

    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userOrgDF)
    val allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsDF.join(userRatingDF, Seq("courseID", "userID"), "left")

    allCourseProgramCompletionWithDetailsDFWithRating
      .durationFormat("courseDuration")
      .withColumn("completedOn", to_date(col("courseCompletedTimestamp"), "dd/MM/yyyy"))
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
      .withColumn("completionPercentage", round(col("completionPercentage"), 2))
      .select(
        col("userID"),
        col("fullName"),
        col("personalDetails.primaryEmail").alias("email"),
        col("professionalDetails.designation").alias("designation"),
        col("userOrgName"),
        col("courseName"),
        col("courseDuration").alias("duration"),
        col("courseOrgName"),
        col("courseLastPublishedOn").alias("lastPublishedOn"),
        col("userCourseCompletionStatus").alias("status"),
        col("completionPercentage"),
        col("completedOn"),
        col("userRating").alias("rating")
      )
      .coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(reportPath + "/userenrollmentrecords")
  }
}
