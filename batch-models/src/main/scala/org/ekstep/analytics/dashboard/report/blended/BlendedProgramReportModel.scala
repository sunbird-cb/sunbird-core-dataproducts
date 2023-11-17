package org.ekstep.analytics.dashboard.report.blended

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtilNew._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object BlendedProgramReportModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.enrolment.BlendedProgramReportModel"

  override def name() = "BlendedProgramReportModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processData(timestamp, config)
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
  def processData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    // get user and user org data
    val orgDF = orgDataFrame()
    val orgHierarchyData = orgHierarchyDataframe()
    var userDataDF = userProfileDetailsDF(orgDF)
      .withColumnRenamed("orgName", "userOrgName")
      .withColumnRenamed("orgCreatedDate", "userOrgCreatedDate")
    userDataDF = userDataDF
      .join(orgHierarchyData, Seq("userOrgName"), "left")
    show(userDataDF, "userDataDF")

    // Get Blended Program data
    val blendedProgramESDF = contentESDataFrame(Seq("Blended Program"), "bp")
    val bpOrgDF = orgDF.select(
      col("orgID").alias("bpOrgID"),
      col("orgName").alias("bpOrgName"),
      col("orgCreatedDate").alias("bpOrgCreatedDate")
    )
    val bpWithOrgDF = blendedProgramESDF.join(bpOrgDF, Seq("bpOrgID"), "left")

    // add BP batch info
    val batchDF = courseBatchDataFrame()

    val bpBatchDF = batchDF.select(
      col("courseID").alias("bpID"),
      col("batchID").alias("bpBatchID"),
      col("courseBatchName").alias("bpBatchName"),
      col("courseBatchStartDate").alias("bpBatchStartDate"),
      col("courseBatchEndDate").alias("bpBatchEndDate"),
      col("courseBatchAttrs").alias("bpBatchAttrs")
    )
      .withColumn("bpBatchAttrs", from_json(col("bpBatchAttrs"), Schema.batchAttrsSchema))
      .withColumn("bpBatchLocation", col("bpBatchAttrs.batchLocationDetails"))
      .withColumn("bpBatchSessionType", col("bpBatchAttrs.sessionType"))  // TODO
      .drop("bpBatchAttrs")

    val relevantBatchInfoDF = bpWithOrgDF.select("bpID", "bpCategory")
      .join(bpBatchDF, Seq("bpID"), "left")
      .select("bpID", "bpBatchID", "bpBatchName", "bpBatchStartDate", "bpBatchEndDate", "bpBatchLocation", "bpBatchSessionType")
    show(relevantBatchInfoDF, "relevantBatchInfoDF")

    val bpWithBatchDF = bpWithOrgDF.join(relevantBatchInfoDF, Seq("bpID"), "left")
    show(bpWithBatchDF, "bpWithBatchDF")

    // add BP user progress info

    // get enrolment table
    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    show(userEnrolmentDF, "userEnrolmentDF")

    val bpUserEnrolmentDF = userEnrolmentDF.select(
      col("userID"),
      col("courseID").alias("bpID"),
      col("batchID").alias("bpBatchID"),
      col("courseEnrolledTimestamp").alias("bpEnrolledTimestamp"),
      col("issuedCertificateCount").alias("bpIssuedCertificateCount"),
      col("dbCompletionStatus")
    )

    val bpCompletionDF = bpWithBatchDF.join(bpUserEnrolmentDF, Seq("bpID", "bpBatchID"), "inner")

    // add user and user org info
    var bpCompletionWithUserDetailsDF = bpCompletionDF.join(userDataDF, Seq("userID"), "left")
    bpCompletionWithUserDetailsDF = userCourseCompletionStatus(bpCompletionWithUserDetailsDF)
      .withColumnRenamed("userCourseCompletionStatus", "bpUserCompletionStatus")
      .drop("dbCompletionStatus")
    show(bpCompletionWithUserDetailsDF, "bpCompletionWithUserDetailsDF")

    // children
    val hierarchyDF = contentHierarchyDataFrame()

    val bpIDsDF = blendedProgramESDF.select("bpID")

    // L1 children without modules
    val bpChildL1WithModulesDF = addHierarchyColumn(bpIDsDF, hierarchyDF, "bpID", "data", children = true)
      .withColumn("bpChild", explode_outer(col("data.children")))
      .withColumn("bpChildID", col("bpChild.identifier"))
      .withColumn("bpChildName", col("bpChild.name"))
      .withColumn("bpChildCategory", col("bpChild.category"))
      .withColumn("bpChildResourceCount", col("bpChild.leafNodesCount"))
      .withColumn("bpChildDuration", col("bpChild.duration"))
      .drop("data")
    show(bpChildL1WithModulesDF, "bpChildL1WithModulesDF")

    val bpChildL1DF = bpChildL1WithModulesDF.where(expr("bpChildCategory != 'Course Unit'"))

    // L2 (module) Children
    val bpModuleIDsDF = bpChildL1WithModulesDF.where(expr("bpChildCategory = 'Course Unit'"))
      .select(col("bpID"), col("bpChildID").alias("bpModuleID"))

    val bpChildL2DF = addHierarchyColumn(bpModuleIDsDF, hierarchyDF, "bpModuleID", "data", children = true)
      .withColumn("bpChild", explode_outer(col("data.children")))
      .withColumn("bpChildID", col("bpChild.identifier"))
      .withColumn("bpChildName", col("bpChild.name"))
      .withColumn("bpChildCategory", col("bpChild.category"))
      .withColumn("bpChildResourceCount", col("bpChild.leafNodesCount"))
      .withColumn("bpChildDuration", col("bpChild.duration"))
      .drop("data", "bpModuleID")

    // merge L1 and L2 children
    val bpChildDF = bpChildL1DF.union(bpChildL2DF)

    // add children info to bpCompletionWithUserDetailsDF
    val bpCompletionWithChildrenDF = bpCompletionWithUserDetailsDF.join(bpChildDF, Seq("bpID"), "left")

    // add children batch info
    val bpChildBatchDF = bpBatchDF.select(
      col("bpID").alias("bpChildID"),
      col("bpBatchID").alias("bpChildBatchID"),
      col("bpBatchName").alias("bpChildBatchName"),
      col("bpBatchStartDate").alias("bpChildBatchStartDate"),
      col("bpBatchEndDate").alias("bpChildBatchEndDate"),
      col("bpBatchLocation").alias("bpChildBatchLocation"),
      col("bpBatchSessionType").alias("bpChildBatchSessionType")
    )

    val relevantChildBatchInfoDF = bpChildDF.select("bpChildID", "bpChildCategory")
      .join(bpChildBatchDF, Seq("bpChildID"), "left")
      .select("bpChildID", "bpChildBatchID", "bpChildBatchName", "bpChildBatchStartDate", "bpChildBatchEndDate", "bpChildBatchLocation", "bpChildBatchSessionType")
    show(relevantChildBatchInfoDF, "relevantChildBatchInfoDF")

    val bpCompletionWithChildBatchInfoDF = bpCompletionWithChildrenDF.join(relevantChildBatchInfoDF, Seq("bpChildID", "bpChildBatchID"), "left")
    show(bpCompletionWithChildBatchInfoDF, "bpCompletionWithChildBatchInfoDF")

    // add child progress info
    val bpChildUserEnrolmentDF = userEnrolmentDF.select(
      col("userID"),
      col("courseID").alias("bpChildID"),
      col("courseProgress").alias("bpChildProgress"),
      col("dbCompletionStatus")
    )
    var bpChildrenWithProgress = bpCompletionWithChildBatchInfoDF.join(bpChildUserEnrolmentDF, Seq("userID", "bpChildID"), "left")
    bpChildrenWithProgress = bpChildrenWithProgress
      .withColumn("completionPercentage", expr("CASE WHEN dbCompletionStatus=2 THEN 100.0 WHEN bpChildProgress=0 OR bpChildResourceCount=0 OR dbCompletionStatus=0 THEN 0.0 ELSE 100.0 * bpChildProgress / bpChildResourceCount END"))
      .withColumn("completionPercentage", expr("CASE WHEN completionPercentage > 100.0 THEN 100.0 WHEN completionPercentage < 0.0 THEN 0.0 ELSE completionPercentage END"))
      .withColumnRenamed("completionPercentage", "bpChildProgressPercentage")
    bpChildrenWithProgress = userCourseCompletionStatus(bpChildrenWithProgress)
      .withColumnRenamed("userCourseCompletionStatus", "bpChildUserStatus")
      .withColumn("bpChildAttendanceStatus", expr("CASE WHEN bpChildBatchSessionType != 'Offline' THEN '' WHEN dbCompletionStatus IN (0, 1, 2) THEN 'Attended' ELSE 'Not Attended' END"))

    // finalize report data frame
    var df = bpChildrenWithProgress
      .withColumn("enrolledOn", to_date(col("bpEnrolledTimestamp"), "dd/MM/yyyy"))
      .withColumn("bpBatchStartDate", to_date(col("bpBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("bpBatchEndDate", to_date(col("bpBatchEndDate"), "dd/MM/yyyy"))
      .withColumn("bpChildBatchStartDate", to_date(col("bpChildBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("bpChildProgressPercentage", round(col("bpChildProgressPercentage"), 2))
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("Certificate_Generated", expr("CASE WHEN bpIssuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("bpChildOfflineStartDate", expr("CASE WHEN bpChildBatchSessionType = 'Offline' THEN bpChildBatchStartDate ELSE '' END"))

    df = df
      .select(
        col("userID"),
        col("userOrgID"),
        col("bpID"),
        col("bpOrgID"),
        col("bpChildID"),
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("professionalDetails.group").alias("Group"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        col("bpOrgName").alias("Provider_Name"),
        col("courseName").alias("Blended Program_Name"),
        col("bpBatchID").alias("Batch_Id"),
        col("bpBatchName").alias("Batch_Name"),
        col("bpBatchLocation").alias("Batch_Location"),
        col("bpBatchStartDate").alias("Batch_Start_Date"),
        col("bpBatchEndDate").alias("Batch_End_Date"),
        col("enrolledOn").alias("Enrolled_On"),

        col("bpChildName").alias("Component_Name"),
        col("bpChildCategory").alias("Component_Type"),
        col("bpChildBatchSessionType").alias("Component_Mode"),
        col("bpChildUserStatus").alias("Status"),
        col("bpChildOfflineStartDate").alias("Offline_Session_Date"),
        col("bpChildDuration").alias("Component_Duration"),
        col("bpChildProgressPercentage").alias("Component_Progress_Percentage"),

        col("bpChildAttendanceStatus").alias("Offline_Attendance_Status"),
        lit("").alias("Instructor_Name"), // TODO
        lit("").alias("Program_Coordinator_Name"), // TODO
        col("Certificate_Generated"),
        col("userOrgID").alias("mdoid"),
        col("bpIssuedCertificateCount"),
        col("Report_Last_Generated_On")
      )

    show(df, "df")

    df = df.coalesce(1)
    val reportPath = s"${conf.blendedReportPath}/${today}"
    // generateFullReport(df, s"${conf.blendedReportPath}-test/${today}")
    generateFullReport(df, reportPath)
    df = df.drop("userID", "userOrgID", "bpID", "bpOrgID", "bpChildID", "bpIssuedCertificateCount")
    generateAndSyncReports(df, "mdoid", reportPath, "BlendedProgramAttendanceReport")

    closeRedisConnect()
  }
}
