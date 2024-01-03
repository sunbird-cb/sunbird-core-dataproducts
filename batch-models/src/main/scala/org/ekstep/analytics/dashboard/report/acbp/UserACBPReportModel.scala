package org.ekstep.analytics.dashboard.report.acbp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil.{contentDataFrames, _}


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

  def contentDataFrames(orgDF: DataFrame, primaryCategories: Seq[String] = Seq("Course", "Program"), runValidation: Boolean = true)(implicit spark: SparkSession, conf: DashboardConfig): (DataFrame, DataFrame, DataFrame) = {
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

    val acbpDataDF = acbpDetailsDF()
    val orgDF = orgDataFrame()
    val userDataDF = userProfileDetailsDF(orgDF)
    acbpDataDF.printSchema()
    userDataDF.printSchema()
    val userCourseProgramEnrolmentDF = userCourseProgramCompletionDataFrame()
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF) = contentDataFrames(orgDF)



   // CustomUser
    val customUserDF = acbpDataDF.filter(col("assignmenttype") === "CustomUser")
    val explodedAcbDataUserDF = customUserDF.select(col("*"),explode(col("assignmenttypeinfo")).as("userID"))
    val resultCustomUser = getUserDetailsForCustomUser(userDataDF, explodedAcbDataUserDF)

    // Designation
    val designationDF = acbpDataDF.filter(col("assignmenttype") === "Designation")
    val explodedAcbDataDesignationDF = designationDF.select(col("*"), explode(col("assignmenttypeinfo")).as("designation"))
    val resultDesignation = getUserDetailsForDesignation(userDataDF, explodedAcbDataDesignationDF)

    // All User
    val allUserDF = acbpDataDF.filter(col("assignmenttype") === "AllUser")
    val resultAllUser = getUserDetailsForAllUser(userDataDF, allUserDF)

    // union of all the response dfs
    val joinedDF = resultCustomUser.union(resultDesignation).union(resultAllUser)

    // replace content list with names of the courses instead of ids
    val explodedJoinedDF = joinedDF.withColumn("courseID", explode(col("Name of the ACBP Allocated Courses")))
    val resultDF = replaceCourseIdsWithNames(explodedJoinedDF, allCourseProgramDetailsDF)


    // Join finalResultDF and userEnrolmentDF to calculate the Current Progress and Actual date of Completion
    val finalResultDF = resultDF.join(userCourseProgramEnrolmentDF, array_contains(resultDF("Name of the ACBP Allocated Courses"),
        userCourseProgramEnrolmentDF("courseID")) &&
        resultDF("userID") === userCourseProgramEnrolmentDF("userID"), "left_outer")
      .groupBy(resultDF("Name"), resultDF("Masked Email"), resultDF("Masked Phone"), resultDF("Designation"), resultDF("Group"), resultDF("MDO Name"), resultDF("Due Date of Completion"), resultDF("Allocated On"), resultDF("Name of the ACBP Allocated Courses"))
      .agg(
        countDistinct(userCourseProgramEnrolmentDF("dbCompletionStatus")).as("distinctStatusCount"),
        max(userCourseProgramEnrolmentDF("dbCompletionStatus")).as("maxStatus"),
        max(userCourseProgramEnrolmentDF("courseCompletedTimestamp")).as("maxCourseCompletedTimestamp")
      )
      .withColumn("Current Progress",
        when(col("distinctStatusCount") === 1 && col("maxStatus") === 2, "Completed")
          .when(col("distinctStatusCount") === 1 && col("maxStatus") === 1, "In Progress")
          .when(col("distinctStatusCount") === 1 && col("maxStatus") === 0, "Not Started")
          .when(col("distinctStatusCount") > 1, "In Progress")
          .otherwise("Not Enrolled")
      )
      .withColumn("Actual Date of Completion",
        when(col("distinctStatusCount") === 1 && col("maxStatus") === 2, col("maxCourseCompletedTimestamp"))
          .otherwise(lit(""))
      )
      .select(
        col("Name"),
        col("Masked Email"),
        col("Masked Phone"),
        col("Designation"),
        col("Group"),
        col("MDO Name"),
        col("Due Date of Completion"),
        col("Allocated On"),
        col("Name of the ACBP Allocated Courses"),
        col("Current Progress"),
        col("Actual Date of Completion")
      )

    // Fill missing values with ""
    val finalAcbpResultDF = resultDF.na.fill("")

    //while writing it directly to csv, getting an error that says csv data source doesn't support array data type. Therefore, Stringifying it.

    val finalResultStringifiedDFWithCSV = finalResultDF
      .withColumn("Name of the ACBP Allocated Courses", concat_ws(",", col("Name of the ACBP Allocated Courses")))
    show(finalResultStringifiedDFWithCSV, "the final acbp report data")
    //val reportPath = s"${conf.userReportPath}/${today}"
    val reportPath = s"standalone-reports/acbp-user-exhaust/${today}"
    generateFullReport(finalResultStringifiedDFWithCSV, reportPath)
    Redis.closeRedisConnect()

  }
}
