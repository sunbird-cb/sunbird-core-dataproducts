package org.ekstep.analytics.dashboard.report.enrolment

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object UserEnrolmentModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.enrolment.UserEnrolmentModel"

  override def name() = "UserEnrolmentModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    //GET ORG DATA
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()
    val userDataDF = userOrgDF
      .join(orgHierarchyData, Seq("userOrgID"), "left")
    show(userDataDF, "userDataDF")

    // Get course data first
    val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"))

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    show(userEnrolmentDF, "userEnrolmentDF")

    val userRatingDF = userCourseRatingDataframe()

    //use allCourseProgramDetailsDFWithOrgName below instead of allCourseProgramDetailsDF after adding orgname alias above
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userDataDF)
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
      .durationFormat("courseDuration")
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
      .withColumn("Certificate_ID", col("certificateID"))

    df = df.distinct().dropDuplicates("userID", "courseID")

    // read acbp report csv and filter the cbp plan based on status or date
    val CBPlanDetails = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.acbpReportPath}/${today}-warehouse").where(col("status") === "Live")
      .select(col("content_id").alias("courseID"))
      .withColumn("liveCBPlan", lit(true)).distinct()
    // join with acbp report for course ids,
    df = df.join(CBPlanDetails, Seq("courseID"), "left")
      .withColumn("live_cbp_plan_mandate", when(col("liveCBPlan").isNull, false).otherwise(col("liveCBPlan")))

    val fullReportDF = df.select(
      col("userID"),
      col("userOrgID"),
      col("courseID"),
      col("courseOrgID"),
      col("fullName").alias("Full_Name"),
      col("professionalDetails.designation").alias("Designation"),
      col("personalDetails.primaryEmail").alias("Email"),
      col("personalDetails.mobile").alias("Phone_Number"),
      col("professionalDetails.group").alias("Group"),
      col("Tag"),
      col("ministry_name").alias("Ministry"),
      col("dept_name").alias("Department"),
      col("userOrgName").alias("Organization"),
      col("courseOrgName").alias("Content_Provider"),
      col("courseName").alias("Content_Name"),
      col("category").alias("Content_Type"),
      col("courseDuration").alias("Content_Duration"),
      col("batchID").alias("Batch_Id"),
      col("courseBatchName").alias("Batch_Name"),
      col("courseBatchStartDate").alias("Batch_Start_Date"),
      col("courseBatchEndDate").alias("Batch_End_Date"),
      col("enrolledOn").alias("Enrolled_On"),
      col("userCourseCompletionStatus").alias("Status"),
      col("completionPercentage").alias("Content_Progress_Percentage"),
      col("courseLastPublishedOn").alias("Last_Published_On"),
      col("ArchivedOn").alias("Content_Retired_On"),
      col("completedOn").alias("Completed_On"),
      col("Certificate_Generated"),
      col("userRating").alias("User_Rating"),
      col("personalDetails.gender").alias("Gender"),
      col("personalDetails.category").alias("Category"),
      col("additionalProperties.externalSystem").alias("External_System"),
      col("additionalProperties.externalSystemId").alias("External_System_Id"),
      col("userOrgID").alias("mdoid"),
      col("issuedCertificateCount"),
      col("courseStatus"),
      col("courseResourceCount").alias("resourceCount"),
      col("courseProgress").alias("resourcesConsumed"),
      round(expr("CASE WHEN courseResourceCount=0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"), 2).alias("rawCompletionPercentage"),
      col("Certificate_ID"),
      col("Report_Last_Generated_On"),
      col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate")
    )
      .coalesce(1)

    show(df, "df")

    val reportPath = s"${conf.userEnrolmentReportPath}/${today}"
    // generateReport(fullReportDF, s"${reportPath}-full")

    val mdoReportDF = fullReportDF.drop("userID", "userOrgID", "courseID", "courseOrgID", "issuedCertificateCount", "courseStatus", "resourceCount", "resourcesConsumed", "rawCompletionPercentage")
    generateReport(mdoReportDF, reportPath, "mdoid","ConsumptionReport")
    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    }

    val warehouseDF = df
      .withColumn("certificate_generated_on",to_date(from_utc_timestamp(to_utc_timestamp(to_timestamp(
        col("certificateGeneratedOn"), "yyyy-MM-dd'T'HH:mm:ss.SSS"), "UTC"), "IST"), "yyyy-MM-dd"))
      .withColumn("data_last_generated_on", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss a"))
      .select(
        col("userID").alias("user_id"),
        col("batchID").alias("batch_id"),
        col("courseID").alias("content_id"),
        col("completionPercentage").alias("content_progress_percentage"),
        date_format(col("completedOn"), "dd/MM/yyyy HH:mm:ss").alias("last_accessed_on"),
        date_format(col("firstCompletedOn"), "dd/MM/yyyy HH:mm:ss").alias("first_completed_on"),
        col("Certificate_Generated").alias("certificate_generated"),
        date_format(col("certificate_generated_on"), "dd/MM/yyyy HH:mm:ss"),
        col("userRating").alias("user_rating"),
        col("courseProgress").alias("resource_count_consumed"),
        date_format(col("enrolledOn"), "dd/MM/yyyy HH:mm:ss").alias("enrolled_on"),
        col("userCourseCompletionStatus").alias("user_consumption_status"),
        col("Certificate_ID").alias("certificate_id"),
        col("data_last_generated_on"),
        col("live_cbp_plan_mandate")
      ).dropDuplicates("user_id","batch_id","content_id")
    generateReport(warehouseDF.coalesce(1), s"${reportPath}-warehouse")

    Redis.closeRedisConnect()

  }
}
