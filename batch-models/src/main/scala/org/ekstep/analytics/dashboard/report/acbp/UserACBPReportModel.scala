package org.ekstep.analytics.dashboard.report.acbp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._


object UserACBPReportModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.acbp.UserACBPReportModel"
  override def name() = "UserACBPReportModel"
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
    processData(timestamp, config)
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

  def processData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    // get user and org data frames
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val userDataDF = userOrgDF
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))
      .withColumn("group", coalesce(col("professionalDetails.group"), lit("")))
      .withColumn("userPrimaryEmail", col("personalDetails.primaryEmail"))
      .withColumn("userMobile", col("personalDetails.mobile"))
      .select("userID", "fullName", "userPrimaryEmail", "userMobile", "userOrgID", "userOrgName", "designation", "group")
    show(userDataDF, "userDataDF")

    // get course details and course enrolment data frames
    val hierarchyDF = contentHierarchyDataFrame()
    val allCourseProgramESDF = allCourseProgramESDataFrame(Seq("Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"))
    val allCourseProgramDetailsWithCompDF = allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramESDF, hierarchyDF, orgDF)
    val allCourseProgramDetailsDF = allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF)
    val userCourseProgramEnrolmentDF = userCourseProgramCompletionDataFrame()

    // get ACBP details data frame
    val acbpDF = acbpDetailsDF()

   // CustomUser
    val acbpCustomUserAllotmentDF = acbpDF
      .filter(col("assignmentType") === "CustomUser")
      .withColumn("userID", explode(col("assignmentTypeInfo")))
      .join(userDataDF, Seq("userID", "userOrgID"), "left")
    show(acbpCustomUserAllotmentDF, "acbpCustomUserAllotmentDF")

    // Designation
    val acbpDesignationAllotmentDF = acbpDF
      .filter(col("assignmentType") === "Designation")
      .withColumn("designation", explode(col("assignmentTypeInfo")))
      .join(userDataDF, Seq("userOrgID", "designation"), "left")
    show(acbpDesignationAllotmentDF, "acbpDesignationAllotmentDF")

    // All User
    val acbpAllUserAllotmentDF = acbpDF
      .filter(col("assignmentType") === "AllUser")
      .join(userDataDF, Seq("userOrgID"), "left")
    show(acbpAllUserAllotmentDF, "acbpAllUserAllotmentDF")

    // union of all the response dfs
    val acbpAllotmentDF = Seq(acbpCustomUserAllotmentDF, acbpDesignationAllotmentDF, acbpAllUserAllotmentDF).map(df => {
      df.select("userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID", "userOrgName", "acbpID", "assignmentType", "completionDueDate", "allocatedOn", "acbpCourseIDList")
    }).reduce((a, b) => a.union(b))
    show(acbpAllotmentDF, "acbpAllotmentDF")

    // replace content list with names of the courses instead of ids
    val acbpAllEnrolmentDF = acbpAllotmentDF
      .withColumn("courseID", explode(col("acbpCourseIDList")))
      .join(allCourseProgramDetailsDF, Seq("courseID"), "left")
      .join(userCourseProgramEnrolmentDF, Seq("courseID", "userID"), "left")
      .na.drop(Seq("userID", "courseID"))
      .drop("acbpCourseIDList")
    show(acbpAllEnrolmentDF, "acbpAllEnrolmentDF")

    // for particular userID and course ID, choose allotment entries based on priority rules
    val acbpEnrolmentDF = acbpAllEnrolmentDF
      .groupByLimit(Seq("userID", "courseID"), "completionDueDate", 1, desc = true)
    kafkaDispatch(withTimestamp(acbpEnrolmentDF, timestamp), conf.acbpEnrolmentTopic)

    // for enrolment report
    val enrolmentReportDataDF = acbpEnrolmentDF
      .withColumn("currentProgress", expr("CASE WHEN dbCompletionStatus=2 THEN 'Completed' WHEN dbCompletionStatus=1 THEN 'In Progress' WHEN dbCompletionStatus=0 THEN 'Not Started' ELSE 'Not Enrolled' END"))
      .withColumn("courseCompletedTimestamp", to_date(col("courseCompletedTimestamp"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("allocatedOn", to_date(col("allocatedOn"), "dd/MM/yyyy"))
      .withColumn("completionDueDate", to_date(col("completionDueDate"), "dd/MM/yyyy"))
      .na.fill("")
    show(enrolmentReportDataDF, "enrolmentReportDataDF")

    val enrolmentReportDF = enrolmentReportDataDF
      .select(
        col("fullName").alias("Name"),
        col("userPrimaryEmail").alias("Email"),
        col("userMobile").alias("Phone"),
        col("userOrgName").alias("MDO Name"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        col("courseName").alias("Name of CBP Allocated Course"),
        col("allocatedOn").alias("Allocated On"),
        col("currentProgress").alias("Current Progress"),
        col("completionDueDate").alias("Due Date of Completion"),
        col("courseCompletedTimestamp").alias("Actual Date of Completion"),
        col("userOrgID").alias("mdoid"),
        date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a").alias("Report_Last_Generated_On")
      )
    show(enrolmentReportDF, "enrolmentReportDF")

    // for user summary report
    val userSummaryDataDF = acbpEnrolmentDF
      .withColumn("completionDueDateLong", col("completionDueDate").cast(LongType))
      .withColumn("courseCompletedTimestampLong", col("courseCompletedTimestamp").cast(LongType))
      .groupBy("userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID", "userOrgName")
      .agg(
        count("courseID").alias("allocatedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 AND courseCompletedTimestampLong<=completionDueDateLong THEN 1 ELSE 0 END)").alias("completedBeforeDueDateCount")
      )
    show(userSummaryDataDF, "userSummaryDataDF")

    val userSummaryReportDF = userSummaryDataDF
      .select(
        col("fullName").alias("Name"),
        col("userPrimaryEmail").alias("Email"),
        col("userMobile").alias("Phone"),
        col("userOrgName").alias("MDO Name"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        col("allocatedCount").alias("Number of ACBP Courses Allocated"),
        col("completedCount").alias("Number of ACBP Courses Completed"),
        col("completedBeforeDueDateCount").alias("Number of ACBP Courses Completed within due date"),
        col("userOrgID").alias("mdoid"),
        date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a").alias("Report_Last_Generated_On")
      )
    show(userSummaryReportDF, "userSummaryReportDF")

    val reportPath = s"${conf.acbpReportPath}/${today}"
    generateFullReport(acbpAllEnrolmentDF.coalesce(1), reportPath)

    generateReportsWithoutPartition(enrolmentReportDF.drop("mdoid"), s"${reportPath}/ACBPEnrollmentReport", "ACBPEnrollmentReport")
    generateReportsWithoutPartition(userSummaryReportDF.drop("mdoid"), s"${reportPath}/ACBPUserSummaryReport", "ACBPUserSummaryReport")
    syncReports(s"/tmp/${reportPath}", reportPath)

    generateAndSyncReports(enrolmentReportDF.coalesce(1), "mdoid", s"${conf.acbpMdoEnrolmentReportPath}/${today}", "ACBPEnrollmentReport")
    generateAndSyncReports(userSummaryReportDF.coalesce(1), "mdoid", s"${conf.acbpMdoSummaryReportPath}/${today}", "ACBPUserSummaryReport")

    Redis.closeRedisConnect()

  }
}

