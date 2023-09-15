package org.ekstep.analytics.dashboard.report.enrolment_new

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtilNew._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object UserEnrolmentModelNew extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

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

    val orgHierarchyData = orgHierarchyDataframe()

    //Get course data first
    val allCourseProgramDetailsDF = contentDataFrames(false, true)
    val allCourseProgramDetailsDFWithOrgName = allCourseProgramDetailsDF.join(orgDF, allCourseProgramDetailsDF.col("courseActualOrgId").equalTo(orgDF.col("orgID")), "left")
      .withColumnRenamed("orgName", "courseOrgName")
    show(allCourseProgramDetailsDFWithOrgName, "allCourseProgramDetailsDFWithOrgName")
    //add alias for orgName col to avoid conflict with userDF orgName col

    //allCourseProgramDetailsDFWithOrgName.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save( reportPath + "/allCourseProgramDetailsDF" )

    //GET ORG DATW
    var userDataDF = userProfileDetailsDF(orgDF).withColumn("fullName", col("firstName"))
      .withColumnRenamed("orgName", "userOrgName")
      .withColumnRenamed("orgCreatedDate", "userOrgCreatedDate")
    show(userDataDF, "userDataDF")

    userDataDF = userDataDF
      .join(orgHierarchyData, Seq("userOrgName"), "left")

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()

    val userRatingDF = userCourseRatingDataframe()

    //use allCourseProgramDetailsDFWithOrgName below instead of allCourseProgramDetailsDF after adding orgname alias above
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDFWithOrgName, userDataDF)
    val allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsDF.join(userRatingDF, Seq("courseID", "userID"), "left")

    val finalDF = allCourseProgramCompletionWithDetailsDFWithRating
      .withColumn("completedOn", to_date(col("courseCompletedTimestamp"), "dd/MM/yyyy"))
      .withColumn("enrolledOn", to_date(col("courseEnrolledTimestamp"), "dd/MM/yyyy"))
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
      .withColumn("completionPercentage", round(col("completionPercentage"), 2))
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .select(
        col("userID"),
        col("fullName").alias("Full_Name"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_NUmber"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("professionalDetails.designation").alias("Designation"),
        col("professionalDetails.group").alias("Group"),
        col("Tag").alias("Tag"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("userOrgName").alias("Organization"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("courseName").alias("CBP_Name"),
        col("category").alias("CBP_Type"),
        // col("issuedCertificates").alias("Certificate_Generated"),
        col("courseDuration").alias("CBP_Duration"),
        col("courseOrgName").alias("CBP_Provider"),
        col("courseLastPublishedOn").alias("Last_Published_On"),
        col("userCourseCompletionStatus").alias("Status"),
        col("completionPercentage").alias("CBP_Progress_Percentage"),
        col("completedOn").alias("Completed_On"),
        col("userRating").alias("User_Rating"),
        col("userOrgID").alias("mdoid")
      )
      .withColumn("Batch_Start_Date", lit(""))
      .withColumn("Batch_End_Date", lit(""))
      .withColumn("Batch_ID", lit(""))
      .withColumn("Batch_Name", lit(""))

    show(finalDF, "finalDF")

    uploadReports(finalDF, "mdoid", reportPath, s"${conf.userEnrolmentReportPath}/${today}/")

    //.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save( reportPath + "/userenrollmentrecords" )
  }
}
