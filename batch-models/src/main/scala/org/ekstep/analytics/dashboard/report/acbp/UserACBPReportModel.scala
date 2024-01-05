package org.ekstep.analytics.dashboard.report.acbp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
    processUserACBPReportModel(timestamp, config)
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

  def contentDataFrames2(orgDF: DataFrame, primaryCategories: Seq[String] = Seq("Course", "Program"), runValidation: Boolean = true)(implicit spark: SparkSession, conf: DashboardConfig): (DataFrame, DataFrame, DataFrame) = {
    val allowedCategories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment","Curated Program")
    val notAllowed = primaryCategories.toSet.diff(allowedCategories.toSet)
    if (notAllowed.nonEmpty) {
      throw new Exception(s"Category not allowed: ${notAllowed.mkString(", ")}")
    }

    val hierarchyDF = contentHierarchyDataFrame()
    val allCourseProgramESDF = allCourseProgramESDataFrame(primaryCategories)
    val allCourseProgramDetailsWithCompDF = allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramESDF, hierarchyDF, orgDF)
    val allCourseProgramDetailsDF = allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF)

    (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF)
  }

  def processUserACBPReportModel(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    val acbpDataDF = acbpDetailsDF().withColumn("userOrgID", col("orgid"))
    val orgDF = orgDataFrame()
    val userDataDF = userProfileDetailsDF(orgDF)
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))
      .withColumn("group", coalesce(col("professionalDetails.group"), lit("")))
    val userCourseProgramEnrolmentDF = userCourseProgramCompletionDataFrame()
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF) = contentDataFrames2(orgDF)

    show(userDataDF, "userDataDF")

   // CustomUser
    val customUserDF = acbpDataDF.filter(col("assignmenttype") === "CustomUser")
    val explodedAcbDataUserDF = customUserDF.select(col("*"), explode(col("assignmenttypeinfo")).alias("userID"))
    show(explodedAcbDataUserDF, "explodedAcbDataUserDF")

    val resultCustomUser = explodedAcbDataUserDF.join(userDataDF, Seq("userID", "userOrgID"), "left")
      .select(
        col("userID"),
        col("fullName"),
        col("maskedEmail"),
        col("maskedPhone"),
        col("userOrgID"),
        col("userOrgName"),
        col("designation"),
        col("group"),
        col("enddate").alias("completionDueDate"),
        col("id").alias("ACBCourseID"),
        col("publishedat").alias("allocatedOn"),
        col("contentlist").alias("ACBCourseIDList")
      )
    show(resultCustomUser, "resultCustomUser")

    // Designation
    val designationDF = acbpDataDF.filter(col("assignmenttype") === "Designation")
    val explodedAcbDataDesignationDF = designationDF.select(col("*"), explode(col("assignmenttypeinfo")).alias("designation"))
    show(explodedAcbDataDesignationDF, "explodedAcbDataDesignationDF")

    val resultDesignation = explodedAcbDataDesignationDF.join(userDataDF, Seq("userOrgID", "designation"), "inner")
      .select(
        col("userID"),
        col("fullName"),
        col("maskedEmail"),
        col("maskedPhone"),
        col("userOrgID"),
        col("userOrgName"),
        col("designation"),
        col("group"),
        col("enddate").alias("completionDueDate"),
        col("id").alias("ACBCourseID"),
        col("publishedat").alias("allocatedOn"),
        col("contentlist").alias("ACBCourseIDList")
      )
    show(resultDesignation, "resultDesignation")

    // All User
    val allUserDF = acbpDataDF.filter(col("assignmenttype") === "AllUser")
    show(allUserDF, "allUserDF")
    val resultAllUser = allUserDF.join(userDataDF, Seq("userOrgID"), "inner").dropDuplicates("userID")
      .select(
        col("userID"),
        col("fullName"),
        col("maskedEmail"),
        col("maskedPhone"),
        col("userOrgID"),
        col("userOrgName"),
        col("designation"),
        col("group"),
        col("enddate").alias("completionDueDate"),
        col("id").alias("ACBCourseID"),
        col("publishedat").alias("allocatedOn"),
        col("contentlist").alias("ACBCourseIDList")
      )
    show(resultAllUser, "resultAllUser")

    // union of all the response dfs
    val joinedDF = resultCustomUser.union(resultDesignation).union(resultAllUser)
    show(joinedDF, "joinedDF")

    // replace content list with names of the courses instead of ids
    val explodedJoinedDF = joinedDF.withColumn("courseID", explode(col("ACBCourseIDList")))
    val resultDF = explodedJoinedDF.join(allCourseProgramDetailsDF, Seq("courseID"), "left")
    show(resultDF, "resultDF")

    val finalResultDF = resultDF.join(userCourseProgramEnrolmentDF, Seq("courseID", "userID"), "left")
    show(finalResultDF, "finalResultDF")

    val enrollmentDataDF = finalResultDF
      .groupBy("userID", "fullName", "maskedEmail", "maskedPhone", "designation", "group", "userOrgID",
        "userOrgName", "completionDueDate", "allocatedOn")
      .agg(
        collect_list("courseName").alias("ACBCourseNameList"),
        countDistinct(userCourseProgramEnrolmentDF("dbCompletionStatus")).as("distinctStatusCount"),
        max(userCourseProgramEnrolmentDF("dbCompletionStatus")).as("maxStatus"),
        max(userCourseProgramEnrolmentDF("courseCompletedTimestamp")).as("maxCourseCompletedTimestamp")
      )
      .withColumn("currentProgress",
        when(col("distinctStatusCount") === 1 && col("maxStatus") === 2, "Completed")
          .when(col("distinctStatusCount") === 1 && col("maxStatus") === 1, "In Progress")
          .when(col("distinctStatusCount") === 1 && col("maxStatus") === 0, "Not Started")
          .when(col("distinctStatusCount") > 1, "In Progress")
          .otherwise("Not Enrolled")
      )
      .withColumn("completionDate",
        when(col("distinctStatusCount") === 1 && col("maxStatus") === 2, col("maxCourseCompletedTimestamp"))
          .otherwise(lit(""))
      )
      .na.fill("")
    show(enrollmentDataDF, "enrollmentDataDF")

    val enrollmentReportDF = enrollmentDataDF
      .select(
        col("fullName").alias("Name"),
        col("maskedEmail").alias("Masked Email"),
        col("maskedPhone").alias("Masked Phone"),
        col("userOrgName").alias("MDO Name"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        concat_ws(",", col("ACBCourseNameList")).alias("Name of the ACBP Allocated Courses"),
        col("allocatedOn").alias("Allocated On"),
        col("currentProgress").alias("Current Progress"),
        col("completionDueDate").alias("Due Date of Completion"),
        col("completionDate").alias("Actual Date of Completion")
      )
    show(enrollmentReportDF, "enrollmentReportDF")

    val userSummaryDataDF = finalResultDF
      .groupBy("userID", "fullName", "maskedEmail", "maskedPhone", "designation", "group", "userOrgID", "userOrgName")
      .agg(
        count("courseID").alias("allocatedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 AND completionDate<=completionDueDate THEN 1 ELSE 0 END)").alias("completedBeforeDueDateCount")
      )

    val userSummaryReportDF = userSummaryDataDF
      .select(
        col("fullName").alias("Name"),
        col("maskedEmail").alias("Masked Email"),
        col("maskedPhone").alias("Masked Phone"),
        col("userOrgName").alias("MDO Name"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        col("allocatedCount").alias("Number of ACBP Courses Allocated"),
        col("completedCount").alias("Number of ACBP Courses Completed"),
        col("completedBeforeDueDateCount").alias("Number of ACBP Courses Completed within due date")
      )
    show(userSummaryReportDF, "userSummaryReportDF")

    //val reportPath = s"${conf.userReportPath}/${today}"
    val enrollmentReportPath = s"standalone-reports/acbp-enrollment-exhaust/${today}"
    generateReportsWithoutPartition(enrollmentReportDF, enrollmentReportPath, "ACBPEnrollmentReport")

    //val reportPath = s"${conf.userReportPath}/${today}"
    val userReportPath = s"standalone-reports/acbp-user-summary-exhaust/${today}"
    generateReportsWithoutPartition(userSummaryReportDF, userReportPath, "ACBPUserSummaryReport")

    syncReports(s"/tmp/${enrollmentReportPath}", enrollmentReportPath)
    syncReports(s"/tmp/${userReportPath}", userReportPath)

    Redis.closeRedisConnect()

  }
}
