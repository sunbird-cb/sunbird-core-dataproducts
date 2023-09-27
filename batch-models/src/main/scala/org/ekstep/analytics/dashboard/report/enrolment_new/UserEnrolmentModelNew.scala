package org.ekstep.analytics.dashboard.report.enrolment_new

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
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

    //GET ORG DATA
    val orgDF = orgDataFrame()
    val orgHierarchyData = orgHierarchyDataframe()
    var userDataDF = userProfileDetailsDF(orgDF)
      .withColumnRenamed("orgName", "userOrgName")
      .withColumnRenamed("orgCreatedDate", "userOrgCreatedDate")
    userDataDF = userDataDF
      .join(orgHierarchyData, Seq("userOrgName"), "left")
    show(userDataDF, "userDataDF")

    //Get course data first
    val allCourseProgramDetailsDF = contentDataFrames(false, true)
    val allCourseProgramDetailsDFWithOrgName = allCourseProgramDetailsDF
      .join(orgDF, allCourseProgramDetailsDF.col("courseActualOrgId").equalTo(orgDF.col("orgID")), "left")
      .withColumnRenamed("orgName", "courseOrgName")
    show(allCourseProgramDetailsDFWithOrgName, "allCourseProgramDetailsDFWithOrgName")

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    show(userEnrolmentDF, "userEnrolmentDF")

    val userRatingDF = userCourseRatingDataframe()

    //use allCourseProgramDetailsDFWithOrgName below instead of allCourseProgramDetailsDF after adding orgname alias above
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDFWithOrgName, userDataDF)
    show(allCourseProgramCompletionWithDetailsDF, "allCourseProgramCompletionWithDetailsDF")

    val courseBatchDF = courseBatchDataFrame()
    val relevantBatchInfoDF = allCourseProgramDetailsDF.select("courseID", "category")
      .where(expr("category IN ('Blended Program')"))
      .join(courseBatchDF, Seq("courseID"), "left")
      .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
    show(relevantBatchInfoDF, "relevantBatchInfoDF")

    val allCourseProgramCompletionWithDetailsWithBatchInfoDF = allCourseProgramCompletionWithDetailsDF.join(relevantBatchInfoDF, Seq("courseID", "batchID"), "left")
    show(allCourseProgramCompletionWithDetailsWithBatchInfoDF, "allCourseProgramCompletionWithDetailsWithBatchInfoDF")

    val allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsWithBatchInfoDF.join(userRatingDF, Seq("courseID", "userID"), "left")

    var df = allCourseProgramCompletionWithDetailsDFWithRating
      .withColumn("completedOn", to_date(col("courseCompletedTimestamp"), "dd/MM/yyyy"))
      .withColumn("enrolledOn", to_date(col("courseEnrolledTimestamp"), "dd/MM/yyyy"))
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
      .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), "dd/MM/yyyy"))
      .withColumn("completionPercentage", round(col("completionPercentage"), 2))
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("Certificate_Generated", expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("ArchivedOn", expr("CASE WHEN courseStatus == 'Retired' THEN lastStatusChangedOn ELSE '' END"))
      .withColumn("ArchivedOn", to_date(col("ArchivedOn"), "dd/MM/yyyy"))

    df = df.distinct().dropDuplicates("userID", "courseID")
      .select(
        col("userID"),
        col("userOrgID"),
        col("courseID"),
        col("courseActualOrgId"),
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        col("courseOrgName").alias("CBP_Provider"),
        col("courseName").alias("CBP_Name"),
        col("category").alias("CBP_Type"),
        col("courseDuration").alias("CBP_Duration"),
        col("batchID").alias("Batch_Id"),
        col("courseBatchName").alias("Batch_Name"),
        col("courseBatchStartDate").alias("Batch_Start_Date"),
        col("courseBatchEndDate").alias("Batch_End_Date"),
        col("enrolledOn").alias("Enrolled_On"),
        col("userCourseCompletionStatus").alias("Status"),
        col("completionPercentage").alias("CBP_Progress_Percentage"),
        col("courseLastPublishedOn").alias("Last_Published_On"),
        col("ArchivedOn").alias("CBP_Archived_On"),
        col("completedOn").alias("Completed_On"),
        col("Certificate_Generated"),
        col("userRating").alias("User_Rating"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("userOrgID").alias("mdoid"),
        col("Report_Last_Generated_On")
      )

    show(df, "df")

    df = df.coalesce(1)
    val reportPath = s"${conf.userEnrolmentReportPath}/${today}"
    generateFullReport(df, reportPath)
    // generateFullReport(df, s"${conf.userEnrolmentReportPath}-test/${today}")
    df = df.drop("userID", "userOrgID", "courseID", "courseActualOrgId")
    generateAndSyncReports(df, "mdoid", reportPath, "ConsumptionReport")

    closeRedisConnect()
  }
}
