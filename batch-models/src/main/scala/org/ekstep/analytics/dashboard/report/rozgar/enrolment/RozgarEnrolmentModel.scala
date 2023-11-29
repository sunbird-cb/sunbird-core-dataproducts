package org.ekstep.analytics.dashboard.report.rozgar.enrolment

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object RozgarEnrolmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.report.rozgar.enrolment.RozgarEnrolmentModel"

  override def name() = "RozgarEnrolmentModel"

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

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val userDataDF = userProfileDetailsDF(orgDF)

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF, allCourseProgramDetailsWithRatingDF)=
      contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Standalone Assessment"))

    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userOrgDF)
      .select(col("courseID"), col("userID"), col("completionPercentage"))

    // get the mdoids for which the report are requesting
    val mdoID = conf.mdoIDs
    val mdoIDDF = mdoIDsDF(mdoID)
    val mdoData = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("userOrgID"), col("orgName"))

    val userRating = userCourseRatingDataframe()
    val allCourseData = allCourseProgramDetailsWithRatingDF.join(userEnrolmentDF, Seq("courseID"), "inner")
    //val courseBatchDF = courseBatchDataFrame()

    val orgHierarchyData = orgHierarchyDataframe()
    var df = allCourseData.join(userDataDF, Seq("userID"), "inner").join(mdoData, Seq("userOrgID"), "inner")
      .join(allCourseProgramCompletionWithDetailsDF, Seq("courseID", "userID"), "inner")
      //.join(courseBatchDF, Seq("courseID"), "left")
      .join(userRating, Seq("courseID", "userID"), "left").join(orgHierarchyData, Seq("userOrgName"),"left")

    df = df.withColumn("courseCompletionPercentage", round(col("completionPercentage"), 2))

    df = userCourseCompletionStatus(df)

    df = df.durationFormat("courseDuration", "CBP_Duration")

    val caseExpressionBatchStartDate = "CASE WHEN courseBatchEnrolmentType == 'open' THEN 'Null' ELSE courseBatchStartDate END"
    val caseExpressionBatchEndDate = "CASE WHEN courseBatchEnrolmentType == 'open' THEN 'Null' ELSE courseBatchEndDate END"

    df = df.withColumn("Batch_Start_Date", expr(caseExpressionBatchStartDate))
    df = df.withColumn("Batch_End_Date", expr(caseExpressionBatchEndDate))

    val userConsumedcontents = df.select(
      col("courseID"),
      col("userID"),
      explode_outer(col("courseContentStatus"))
    ).toDF("courseID", "userID", "userContents", "userContentsValue")

    val liveContents = leafNodesDataframe(allCourseProgramCompletionWithDetailsDF, hierarchyDF).select(
      col("liveContentCount"),
      col("identifier").alias("courseID"),
      explode_outer(col("liveContents")).alias("userContents")
    )

    val userConsumedLiveContents = liveContents.join(userConsumedcontents,
      Seq("userContents", "courseID"), "inner")
      .groupBy("courseID", "userID")
      .agg(countDistinct("userContents").alias("currentlyLiveContents"))

    df = df.join(userConsumedLiveContents, Seq("courseID", "userID"), "left")

    val caseExpression = "CASE WHEN userCourseCompletionStatus == 'completed' THEN 100 " +
      "WHEN userCourseCompletionStatus == 'not-started' THEN 0 WHEN userCourseCompletionStatus == 'in-progress' THEN 100 * currentlyLiveContents / courseResourceCount END"
    df = df.withColumn("Completion Percentage", round(expr(caseExpression), 2))

    val caseExpressionCertificate = "CASE WHEN issuedCertificates == '[]' THEN 'No' ELSE 'Yes' END"
    df = df.withColumn("Certificate_Generated", expr(caseExpressionCertificate))

    df = df.withColumn("User_Tag", explode_outer(col("additionalProperties.tag"))).filter(col("User_Tag") === "Rozgar Mela")
    df.show()
    df = df.distinct().dropDuplicates("userID", "courseID").select(
      col("fullName").alias("Full_Name"),
      col("professionalDetails.designation").alias("Designation"),
      col("personalDetails.primaryEmail").alias("Email"),
      col("personalDetails.mobile").alias("Phone_Number"),
      col("professionalDetails.group").alias("Group"),
//      col("User_Tag"),
      col("additionalProperties.tag").alias("Tags").cast("string"),
      col("ministry_name").alias("Ministry"),
      col("dept_name").alias("Department"),
      col("userOrgName").alias("Organization"),
      col("courseOrgName").alias("CBP Provider"),
      col("courseName").alias("CBP Name"),
      col("category").alias("CBP Type"),
      col("CBP_Duration"),
      col("courseBatchID").alias("Batch_ID"),
      col("courseBatchName").alias("Batch_Name"),
      col("Batch_Start_Date"),
      col("Batch_End_Date"),
      from_unixtime(col("courseLastPublishedOn").cast("long"),"dd/MM/yyyy").alias("Last_Published_On"),
      col("userCourseCompletionStatus").alias("Status"),
      col("Completion Percentage").alias("CBP_Progress_Percentage"),
      from_unixtime(col("courseEnrolledTimestamp"),"dd/MM/yyyy").alias("Enrolled_On"),
      from_unixtime(col("courseCompletedTimestamp"),"dd/MM/yyyy").alias("Completed_On"),
      col("Certificate_Generated"),
      col("userRating").alias("Rating"),
      col("personalDetails.gender").alias("Gender"),
      col("personalDetails.category").alias("Category"),
      col("additionalProperties.externalSystem").alias("External System"),
      col("additionalProperties.externalSystemId").alias("External System Id"),
      col("userOrgID").alias("mdoid")
    )

    val reportPath = s"/tmp/${conf.userEnrolmentReportPath}/${today}/"
    val taggedUsersPath = s"${reportPath}${conf.taggedUsersPath}"
    df = df.coalesce(1)
    csvWrite(df, s"/tmp/${conf.userEnrolmentReportPath}/${today}/full/")
    generateAndSyncReports(df, "mdoid", s"${conf.userEnrolmentReportPath}/${today}/${conf.taggedUsersPath}", "RozgarConsumptionReport")

    Redis.closeRedisConnect()
  }
}
