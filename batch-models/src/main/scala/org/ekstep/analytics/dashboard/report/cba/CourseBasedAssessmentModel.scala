package org.ekstep.analytics.dashboard.report.cba

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object CourseBasedAssessmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.cba.CourseBasedAssessmentModel"
  override def name() = "CourseBasedAssessmentModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCourseBasedAssessmentData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }


  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processCourseBasedAssessmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // get course details, with rating info
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Standalone Assessment", "Curated Program"), runValidation = false)

    val assessmentDF = assessmentESDataFrame(Seq("Course", "Standalone Assessment", "Blended Program"))
    val assessWithHierarchyDF = assessWithHierarchyDataFrame(assessmentDF, hierarchyDF, orgDF)
    val assessWithDetailsDF = assessWithHierarchyDF.drop("children")

    val assessChildrenDF = assessmentChildrenDataFrame(assessWithHierarchyDF)
    val userAssessmentDF = userAssessmentDataFrame().filter(col("assessUserStatus") === "SUBMITTED")
    val userAssessChildrenDF = userAssessmentChildrenDataFrame(userAssessmentDF, assessChildrenDF)
    val userAssessChildrenDetailsDF = userAssessmentChildrenDetailsDataFrame(userAssessChildrenDF, assessWithDetailsDF,
      allCourseProgramDetailsWithRatingDF, userOrgDF)

    val orgHierarchyData = orgHierarchyDataframe()

    val userAssessChildDataDF = userAssessChildrenDetailsDF
      .join(orgHierarchyData, Seq("userOrgName"), "left")
    show(userAssessChildDataDF, "userAssessChildDataDF")

    val retakesDF = userAssessChildDataDF
      .groupBy("assessChildID", "userID")
      .agg(countDistinct("assessStartTime").alias("retakes"))
    show(retakesDF, "retakesDF")

    val userAssessChildDataLatestDF = userAssessChildDataDF
      .groupByLimit(Seq("assessChildID", "userID"), "assessEndTimestamp", 1, desc = true)
      .drop("rowNum")
      .join(retakesDF, Seq("assessChildID", "userID"), "left")
    show(userAssessChildDataLatestDF, "userAssessChildDataLatestDF")

    val finalDF = userAssessChildDataLatestDF
      .withColumn("userAssessmentDuration", unix_timestamp(col("assessEndTimestamp")) - unix_timestamp(col("assessStartTimestamp")))
      .withColumn("Pass", expr("CASE WHEN assessPass == 1 THEN 'Yes' ELSE 'No' END"))
      .durationFormat("assessExpectedDuration", "totalAssessmentDuration")
      .withColumn("assessPercentage", when(col("assessPassPercentage").isNotNull, col("assessPassPercentage"))
        .otherwise(lit("Need to pass in all sections")))
      .withColumn("assessment_type", when(col("assessCategory") === "Standalone Assessment", col("assessCategory"))
        .when(col("assessPrimaryCategory").isNotNull, col("assessPrimaryCategory"))
        .otherwise(lit("")))
      .withColumn("assessment_course_name", when(col("assessment_type") === "Course Assessment", col("assessName"))
        .otherwise(lit("")))
      .withColumn("Total_Score_Calculated", when(col("assessMaxQuestions").isNotNull, col("assessMaxQuestions") * 1))
      .withColumn("course_id", when(col("assessCategory") === "Standalone Assessment", lit(""))
        .otherwise(col("assessID")))
      .withColumn("Tags", concat_ws(", ", col("additionalProperties.tag")))
    show(finalDF, "finalDF")

    val fullReportDF = finalDF
      .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))
      .select(
        col("userID").alias("User ID"),
        col("assessID"),
        col("assessOrgID"),
        col("assessChildID"),
        col("userOrgID"),
        col("fullName").alias("Full Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("E mail"),
        col("personalDetails.mobile").alias("Phone Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tags"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organisation"),
        col("assessChildName").alias("Assessment Name"),
        col("assessment_type").alias("Assessment Type"),
        col("assessOrgName").alias("Assessment/Content Provider"),
        from_unixtime(col("assessLastPublishedOn").cast("long"), "dd/MM/yyyy").alias("Assessment Publish Date"),
        col("assessment_course_name").alias("Course Name"),
        col("course_id").alias("Course ID"),
        col("totalAssessmentDuration").alias("Assessment Duration"),
        from_unixtime(col("assessEndTime"), "dd/MM/yyyy").alias("Last Attempted Date"),
        col("assessOverallResult").alias("Latest Percentage Achieved"),
        col("assessPercentage").alias("Cut off Percentage"),
        col("Pass"),
        col("assessMaxQuestions").alias("Total Questions"),
        col("assessIncorrect").alias("No.of Incorrect Responses"),
        col("assessBlank").alias("Unattempted Questions"),
        col("retakes").alias("No. of Retakes"),
        col("userOrgID").alias("mdoid"),
        col("Report_Last_Generated_On")
      )
      .coalesce(1)
    show(fullReportDF, "fullReportDF")

    val reportPath = s"${conf.cbaReportPath}/${today}"
    generateFullReport(fullReportDF, reportPath, conf.localReportDir)

    val mdoReportDF = fullReportDF.drop("assessID", "assessOrgID", "assessChildID", "userOrgID")
    generateAndSyncReports(mdoReportDF, "mdoid", reportPath, "UserAssessmentReport")

    val warehouseDF = finalDF
      .withColumn("data_last_generated_on", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss a"))
      .select(
        col("userID").alias("user_id"),
        col("course_id").alias("content_id"),
        col("assessID").alias("assessment_id"),
        col("assessChildName").alias("assessment_name"),
        col("assessment_type").alias("assessment_type"),
        col("totalAssessmentDuration").alias("assessment_duration"),
        col("totalAssessmentDuration").alias("time_spent_by_the_user"),
        from_unixtime(col("assessEndTime"), "dd/MM/yyyy").alias("completion_date"),
        col("assessOverallResult").alias("score_achieved"),
        col("assessMaxQuestions").alias("overall_score"),
        col("assessPercentage").alias("cut_off_percentage"),
        col("assessMaxQuestions").alias("total_question"),
        col("assessIncorrect").alias("number_of_incorrect_responses"),
        col("retakes").alias("number_of_retakes"),
        col("data_last_generated_on")
      )
    generateWarehouseReport(warehouseDF.coalesce(1), reportPath, conf.localReportDir)

    Redis.closeRedisConnect()

  }
}