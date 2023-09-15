package org.ekstep.analytics.dashboard.report.rozgar_new

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round, to_date}
import org.ekstep.analytics.dashboard.DashboardUtil.{debug, getDate, parseConfig, validation}
import org.ekstep.analytics.dashboard.DataUtilNew._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object RozgarReportModelNew extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.enrolment.UserEnrolmentModelNew"

  override def name() = "UserEnrolmentModelNew"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processUserEnrolmentData(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq()) // return empty rdd
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config    model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processUserEnrolmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val today = getDate()
    val reportPath = s"/tmp/${conf.userEnrolmentReportPath}/${today}/"

    //GET ORG DATA
    val orgDF = orgDataFrame()

    //Get course data first
    val allCourseProgramDetailsDF = contentDataFrames(false, true)
    val allCourseProgramDetailsDFWithOrgName = allCourseProgramDetailsDF.join(orgDF, allCourseProgramDetailsDF.col("courseActualOrgId").equalTo(orgDF.col("orgID")), "left")
      .withColumnRenamed("orgName", "courseOrgName")
    //add alias for orgName col to avoid conflict with userDF orgName col

    //allCourseProgramDetailsDFWithOrgName.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save( reportPath + "/allCourseProgramDetailsDF" )

    //GET ORG DATW
    val userDataDF = userProfileDetailsDF(orgDF).withColumn("fullName", col("firstName"))

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()

    val userRatingDF = userCourseRatingDataframe()

    //use allCourseProgramDetailsDFWithOrgName below instead of allCourseProgramDetailsDF after adding orgname alias above
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDFWithOrgName, userDataDF)
    val allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsDF.join(userRatingDF, Seq("courseID", "userID"), "left")

    allCourseProgramCompletionWithDetailsDFWithRating
      .withColumn("completedOn", to_date(col("courseCompletedTimestamp"), "dd/MM/yyyy"))
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
      .withColumn("completionPercentage", round(col("completionPercentage"), 2))
      .select(
        col("userID"),
        col("fullName"),
        col("personalDetails.primaryEmail").alias("email"),
        col("professionalDetails.designation").alias("designation"),
        col("orgName"),
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
