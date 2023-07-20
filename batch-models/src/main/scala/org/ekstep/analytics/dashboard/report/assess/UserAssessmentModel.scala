package org.ekstep.analytics.dashboard.report.assess

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.framework.{IBatchModelTemplate, _}

import java.io.Serializable
import java.util
import scala.util.matching.Regex

case class CMConfig(
                     debug: String, validation: String,
                     // kafka connection config
                     broker: String, compression: String,
                     // redis connection config
                     redisHost: String, redisPort: Int, redisDB: Int,
                     // other hosts connection config
                     sparkCassandraConnectionHost: String, sparkDruidRouterHost: String,
                     sparkElasticsearchConnectionHost: String, fracBackendHost: String,
                     // kafka topics
                     roleUserCountTopic: String, orgRoleUserCountTopic: String,
                     allCourseTopic: String, userCourseProgramProgressTopic: String,
                     fracCompetencyTopic: String, courseCompetencyTopic: String, expectedCompetencyTopic: String,
                     declaredCompetencyTopic: String, competencyGapTopic: String, userOrgTopic: String,
                     // cassandra key spaces
                     cassandraUserKeyspace: String,
                     cassandraCourseKeyspace: String, cassandraHierarchyStoreKeyspace: String,
                     // cassandra table details
                     cassandraUserTable: String, cassandraUserRolesTable: String, cassandraOrgTable: String,
                     cassandraUserEnrolmentsTable: String, cassandraContentHierarchyTable: String,
                     cassandraRatingSummaryTable: String,
                     // redis keys
                     redisRegisteredOfficerCountKey: String, redisTotalOfficerCountKey: String, redisOrgNameKey: String,
                     redisTotalRegisteredOfficerCountKey: String, redisTotalOrgCountKey: String,
                     redisExpectedUserCompetencyCount: String, redisDeclaredUserCompetencyCount: String,
                     redisUserCompetencyDeclarationRate: String, redisOrgCompetencyDeclarationRate: String,
                     redisUserCompetencyGapCount: String, redisUserCourseEnrollmentCount: String,
                     redisUserCompetencyGapEnrollmentRate: String, redisOrgCompetencyGapEnrollmentRate: String,
                     redisUserCourseCompletionCount: String, redisUserCompetencyGapClosedCount: String,
                     redisUserCompetencyGapClosedRate: String, redisOrgCompetencyGapClosedRate: String
                   ) extends DashboardConfig


object UserAssessmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.CompetencyMetricsModel"
  override def name() = "CompetencyMetricsModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processUserAssessmentData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /***/
  val submitAssessmentRequestSchema: StructType = StructType(Seq(
    StructField("primaryCategory", StringType, nullable = false),
    StructField("courseId", StringType, nullable = false)
  ))

  val submitAssessmentResponseSchema: StructType = StructType(Seq(
    StructField("pass", BooleanType, nullable = false),
    StructField("result", StringType, nullable = false) // check the datatype
  ))

  def userAssessmentDataFrame()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): DataFrame = {

    var df = cassandraTableAsDataFrame("sunbird", "user_assessment_data")
      .select(
        col("userid").alias("assessUserID"),
        col("assessmentid").alias("assessID"),
        col("submitassessmentresponse"),
        col("submitassessmentrequest")
      )
    df = df.withColumn("requestData", from_json(col("submitassessmentrequest"), submitAssessmentRequestSchema))
    df = df.withColumn("responseData", from_json(col("submitassessmentresponse"), submitAssessmentResponseSchema))

    df = df.select(
      col("assessUserID"),
      col("assessID"),
      col("requestData.primaryCategory").alias("assessType"),
      col("requestData.courseId").alias("assessCourseID"),
      col("responseData.pass").alias("assessPass"),
      col("responseData.result").alias("assessPercentageScore")
    )

    show(df)
    df
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processUserAssessmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: CMConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val userAssessmentDF = userAssessmentDataFrame()

    // do course de-norm
    // do user de-norm
    // kafka dispatch to dashboard.assess

    // explode children info also
    // kafka dispatch to dashboard.assess.content

    // filter what is needed in the report
    // write report to blob store




    // obtain and save user org data
    val orgDF = orgDataFrame()
    var userDF = userDataFrame()
    var userOrgDF = userOrgDataFrame(orgDF, userDF)
    // validate userDF and userOrgDF counts
    validate({userDF.count()}, {userOrgDF.count()}, "userDF.count() should equal userOrgDF.count()")

    userDF = userDF.drop("userCreatedTimestamp", "userUpdatedTimestamp")
    userOrgDF = userOrgDF.drop("userCreatedTimestamp", "userUpdatedTimestamp")

//    // obtain and save role count data
//    val roleDF = roleDataFrame()
//    val userOrgRoleDF = userOrgRoleDataFrame(userOrgDF, roleDF)
//    val roleCountDF = roleCountDataFrame(userOrgRoleDF)
//    kafkaDispatch(withTimestamp(roleCountDF, timestamp), conf.roleUserCountTopic)
//
//    // obtain and save org role count data
//    val orgRoleCount = orgRoleCountDataFrame(userOrgRoleDF)
//    kafkaDispatch(withTimestamp(orgRoleCount, timestamp), conf.orgRoleUserCountTopic)

    // org user count
    val orgUserCountDF = orgUserCountDataFrame(orgDF, userDF)
    // validate activeOrgCount and orgUserCountDF count
    validate({orgUserCountDF.count()},
      {userOrgDF.filter(expr("userStatus=1 AND userOrgID IS NOT NULL AND userOrgStatus=1")).select("userOrgID").distinct().count()},
      "orgUserCountDF.count() should equal distinct active org count in userOrgDF")

    // get course details, attach rating info, dispatch to kafka to be ingested by druid data-source: dashboards-courses
    val allCourseProgramESDF = allCourseProgramESDataFrame()
    val allCourseProgramDF = allCourseProgramDataFrame(allCourseProgramESDF, orgDF)
    val allCourseProgramDetailsWithCompDF = allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramDF)
    val allCourseProgramDetailsDF = allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF)
    val courseRatingDF = courseRatingSummaryDataFrame()
    val allCourseProgramDetailsWithRatingDF = allCourseProgramDetailsWithRatingDataFrame(allCourseProgramDetailsDF, courseRatingDF)
    // validate that no rows are getting dropped b/w allCourseProgramESDF and allCourseProgramDetailsWithRatingDF
    validate({allCourseProgramESDF.count()}, {allCourseProgramDetailsWithRatingDF.count()}, "ES course count should equal final DF with rating count")
    // validate that # of rows with ratingSum > 0 in the final DF is equal to # of rows in courseRatingDF from cassandra
    validate(
      {courseRatingDF.where(expr("categoryLower IN ('course', 'program') AND ratingSum > 0")).count()},
      {allCourseProgramDetailsWithRatingDF.where(expr("LOWER(category) IN ('course', 'program') AND ratingSum > 0")).count()},
      "number of ratings in cassandra table for courses and programs with ratingSum > 0 should equal those in final druid datasource")
    // validate rating data, sanity check
    Seq(1, 2, 3, 4, 5).foreach(i => {
      validate(
        {courseRatingDF.where(expr(s"categoryLower IN ('course', 'program') AND ratingAverage <= ${i}")).count()},
        {allCourseProgramDetailsWithRatingDF.where(expr(s"LOWER(category) IN ('course', 'program') AND ratingAverage <= ${i}")).count()},
        s"Rating data row count for courses and programs should equal final DF for ratingAverage <= ${i}"
      )
    })
    kafkaDispatch(withTimestamp(allCourseProgramDetailsWithRatingDF, timestamp), conf.allCourseTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)
    kafkaDispatch(withTimestamp(allCourseProgramCompetencyDF, timestamp), conf.courseCompetencyTopic)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame()
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)
    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")
    kafkaDispatch(withTimestamp(allCourseProgramCompletionWithDetailsDF, timestamp), conf.userCourseProgramProgressTopic)

    val liveCourseCompetencyDF = liveCourseCompetencyDataFrame(allCourseProgramCompetencyDF)

    // get user's expected competency data, dispatch to kafka to be ingested by druid data-source: dashboards-expected-user-competency
    val expectedCompetencyDF = expectedCompetencyDataFrame()
    val expectedCompetencyWithCourseCountDF = expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF, liveCourseCompetencyDF)
    validate({expectedCompetencyDF.count()}, {expectedCompetencyWithCourseCountDF.count()}, "expectedCompetencyDF.count() should equal expectedCompetencyWithCourseCountDF.count()")
    kafkaDispatch(withTimestamp(expectedCompetencyWithCourseCountDF, timestamp), conf.expectedCompetencyTopic)

    // get user's declared competency data, dispatch to kafka to be ingested by druid data-source: dashboards-declared-user-competency
    val declaredCompetencyDF = declaredCompetencyDataFrame()
    kafkaDispatch(withTimestamp(declaredCompetencyDF, timestamp), conf.declaredCompetencyTopic)

    // get frac competency data, dispatch to kafka to be ingested by druid data-source: dashboards-frac-competency
    val fracCompetencyDF = fracCompetencyDataFrame()
    val fracCompetencyWithCourseCountDF = fracCompetencyWithCourseCountDataFrame(fracCompetencyDF, liveCourseCompetencyDF)
    val fracCompetencyWithDetailsDF = fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF, expectedCompetencyDF, declaredCompetencyDF)
    validate({fracCompetencyDF.count()}, {fracCompetencyWithDetailsDF.count()}, "fracCompetencyDF.count() should equal fracCompetencyWithDetailsDF.count()")
    kafkaDispatch(withTimestamp(fracCompetencyWithDetailsDF, timestamp), conf.fracCompetencyTopic)

    // calculate competency gaps, add course completion status, dispatch to kafka to be ingested by druid data-source: dashboards-user-competency-gap
    val competencyGapDF = competencyGapDataFrame(expectedCompetencyDF, declaredCompetencyDF)
    val competencyGapWithCompletionDF = competencyGapCompletionDataFrame(competencyGapDF, liveCourseCompetencyDF, allCourseProgramCompletionWithDetailsDF)  // add course completion status
    validate({competencyGapDF.count()}, {competencyGapWithCompletionDF.count()}, "competencyGapDF.count() should equal competencyGapWithCompletionDF.count()")
    kafkaDispatch(withTimestamp(competencyGapWithCompletionDF, timestamp), conf.competencyGapTopic)

    val liveRetiredCourseCompletionWithDetailsDF = liveRetiredCourseCompletionWithDetailsDataFrame(allCourseProgramCompletionWithDetailsDF)

    // org user details redis dispatch
    val (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap) = getOrgUserMaps(orgUserCountDF)
    val activeOrgCount = orgDF.where(expr("orgStatus=1")).count()
    val activeUserCount = userDF.where(expr("userStatus=1")).count()
    redisDispatch(conf.redisRegisteredOfficerCountKey, orgRegisteredUserCountMap)
    redisDispatch(conf.redisTotalOfficerCountKey, orgTotalUserCountMap)
    redisDispatch(conf.redisOrgNameKey, orgNameMap)
    redisUpdate(conf.redisTotalRegisteredOfficerCountKey, activeUserCount.toString)
    redisUpdate(conf.redisTotalOrgCountKey, activeOrgCount.toString)

    // officer dashboard metrics redis dispatch
    // OL01 - user: expected_competency_count
    val userExpectedCompetencyCountDF = expectedCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"), last("orgID").alias("orgID"))
    show(userExpectedCompetencyCountDF, "OL01")
    redisDispatchDataFrame[Long](conf.redisExpectedUserCompetencyCount, userExpectedCompetencyCountDF, "userID", "count")

    // OL02 - user: declared_competency_count
    val userDeclaredCompetencyCountDF = declaredCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"))
    show(userDeclaredCompetencyCountDF, "OL02")
    redisDispatchDataFrame[Long](conf.redisDeclaredUserCompetencyCount, userDeclaredCompetencyCountDF, "userID", "count")

    // OL03 - user: (declared_competency intersection expected_competency).count / expected_competency_count
    val coveredCompetencyDF = expectedCompetencyDF.join(declaredCompetencyDF, Seq("userID", "competencyID"), "leftouter")
      .na.fill(0, Seq("declaredCompetencyLevel"))
      .where(expr("declaredCompetencyLevel >= expectedCompetencyLevel"))
    val userCoveredCompetencyCountDF = coveredCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("coveredCount"))
    val userCompetencyCoverRateDF = userExpectedCompetencyCountDF.join(userCoveredCompetencyCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("coveredCount"))
      .withColumn("rate", expr("coveredCount / count"))
    show(userCompetencyCoverRateDF, "OL03")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyDeclarationRate, userCompetencyCoverRateDF, "userID", "rate")

    // OL04 - mdo: average_competency_declaration_rate
    val orgCompetencyAvgCoverRateDF = userCompetencyCoverRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyAvgCoverRateDF, "OL04")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyDeclarationRate, orgCompetencyAvgCoverRateDF, "orgID", "rate")

    // OL05 - user: competency gap count
    val userCompetencyGapDF = competencyGapDF.where(expr("competencyGap > 0"))
    val userCompetencyGapCountDF = userCompetencyGapDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"), last("orgID").alias("orgID"))
    show(userCompetencyGapCountDF, "OL05")
    redisDispatchDataFrame[Long](conf.redisUserCompetencyGapCount, userCompetencyGapCountDF, "userID", "count")

    // OL06 - user: enrolled cbp count (IMPORTANT: excluding completed courses)
    val userCourseEnrolledDF = liveRetiredCourseCompletionWithDetailsDF.where(expr("completionStatus in ('started', 'in-progress')"))
    val userCourseEnrolledCountDF = userCourseEnrolledDF.groupBy("userID").agg(
      countDistinct("courseID").alias("count"))
    show(userCourseEnrolledCountDF, "OL06")
    redisDispatchDataFrame[Long](conf.redisUserCourseEnrollmentCount, userCourseEnrolledCountDF, "userID", "count")

    // OL08 - user: competency gaps enrolled percentage (IMPORTANT: excluding completed ones)
    val userCompetencyGapEnrolledDF = competencyGapWithCompletionDF.where(expr("competencyGap > 0 AND completionStatus in ('started', 'in-progress')"))
    val userCompetencyGapEnrolledCountDF = userCompetencyGapEnrolledDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("enrolledCount"))
    val userCompetencyGapEnrolledRateDF = userCompetencyGapCountDF.join(userCompetencyGapEnrolledCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("enrolledCount"))
      .withColumn("rate", expr("enrolledCount / count"))
    show(userCompetencyGapEnrolledRateDF, "OL08")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyGapEnrollmentRate, userCompetencyGapEnrolledRateDF, "userID", "rate")

    // OL09 - mdo: average competency gaps enrolled percentage
    val orgCompetencyGapAvgEnrolledRateDF = userCompetencyGapEnrolledRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyGapAvgEnrolledRateDF, "OL09")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyGapEnrollmentRate, orgCompetencyGapAvgEnrolledRateDF, "orgID", "rate")

    // OL10 - user: completed cbp count
    val userCourseCompletedDF = liveRetiredCourseCompletionWithDetailsDF.where(expr("completionStatus = 'completed'"))
    val userCourseCompletedCountDF = userCourseCompletedDF.groupBy("userID").agg(
      countDistinct("courseID").alias("count"))
    show(userCourseCompletedCountDF, "OL10")
    redisDispatchDataFrame[Long](conf.redisUserCourseCompletionCount, userCourseCompletedCountDF, "userID", "count")

    // OL11 - user: competency gap closed count
    val userCompetencyGapClosedDF = competencyGapWithCompletionDF.where(expr("competencyGap > 0 AND completionStatus = 'completed'"))
    val userCompetencyGapClosedCountDF = userCompetencyGapClosedDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("closedCount"))
    show(userCompetencyGapClosedCountDF, "OL11")
    redisDispatchDataFrame[Long](conf.redisUserCompetencyGapClosedCount, userCompetencyGapClosedCountDF, "userID", "closedCount")

    // OL12 - user: competency gap closed percent
    val userCompetencyGapClosedRateDF = userCompetencyGapCountDF.join(userCompetencyGapClosedCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("closedCount"))
      .withColumn("rate", expr("closedCount / count"))
    show(userCompetencyGapClosedRateDF,  "OL12")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyGapClosedCount, userCompetencyGapClosedRateDF, "userID", "rate")

    // OL13 - mdo: avg competency gap closed percent
    val orgCompetencyGapAvgClosedRateDF = userCompetencyGapClosedRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyGapAvgClosedRateDF, "OL13")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyGapClosedRate, orgCompetencyGapAvgClosedRateDF, "orgID", "rate")

    closeRedisConnect()

  }

  /**
   * OL01 - user: expected_competency_count
   * OL02 - user: declared_competency_count
   * OL03 - user: (declared_competency intersection expected_competency).count / expected_competency_count
   * OL04 - mdo: average_competency_declaration_rate
   * OL05 - user: competency gap count
   * OL06 - user: enrolled cbp count
   * OL08 - user: competency gaps enrolled percentage
   * OL09 - mdo: average competency gaps enrolled percentage
   * OL10 - user: completed cbp count
   * OL11 - user: competency gap closed count
   * OL12 - user: competency gap closed percent
   * OL13 - mdo: avg competency gap closed percent
   */


  def parseConfig(config: Map[String, AnyRef]): CMConfig = {
    CMConfig(
      debug = getConfigModelParam(config, "debug"),
      validation = getConfigModelParam(config, "validation"),
      // kafka connection config
      broker = getConfigSideBroker(config),
      compression = getConfigSideBrokerCompression(config),
      // redis connection config
      redisHost = getConfigModelParam(config, "redisHost"),
      redisPort = getConfigModelParam(config, "redisPort").toInt,
      redisDB = getConfigModelParam(config, "redisDB").toInt,
      // other hosts connection config
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      sparkDruidRouterHost = getConfigModelParam(config, "sparkDruidRouterHost"),
      sparkElasticsearchConnectionHost = getConfigModelParam(config, "sparkElasticsearchConnectionHost"),
      fracBackendHost = getConfigModelParam(config, "fracBackendHost"),
      // kafka topics
      roleUserCountTopic = getConfigSideTopic(config, "roleUserCount"),
      orgRoleUserCountTopic = getConfigSideTopic(config, "orgRoleUserCount"),
      allCourseTopic = getConfigSideTopic(config, "allCourses"),
      userCourseProgramProgressTopic = getConfigSideTopic(config, "userCourseProgramProgress"),
      fracCompetencyTopic = getConfigSideTopic(config, "fracCompetency"),
      courseCompetencyTopic = getConfigSideTopic(config, "courseCompetency"),
      expectedCompetencyTopic = getConfigSideTopic(config, "expectedCompetency"),
      declaredCompetencyTopic = getConfigSideTopic(config, "declaredCompetency"),
      competencyGapTopic = getConfigSideTopic(config, "competencyGap"),
      userOrgTopic = getConfigSideTopic(config, "userOrg"),
      // cassandra key spaces
      cassandraUserKeyspace = getConfigModelParam(config, "cassandraUserKeyspace"),
      cassandraCourseKeyspace = getConfigModelParam(config, "cassandraCourseKeyspace"),
      cassandraHierarchyStoreKeyspace = getConfigModelParam(config, "cassandraHierarchyStoreKeyspace"),
      // cassandra table details
      cassandraUserTable = getConfigModelParam(config, "cassandraUserTable"),
      cassandraUserRolesTable = getConfigModelParam(config, "cassandraUserRolesTable"),
      cassandraOrgTable = getConfigModelParam(config, "cassandraOrgTable"),
      cassandraUserEnrolmentsTable = getConfigModelParam(config, "cassandraUserEnrolmentsTable"),
      cassandraContentHierarchyTable = getConfigModelParam(config, "cassandraContentHierarchyTable"),
      cassandraRatingSummaryTable = getConfigModelParam(config, "cassandraRatingSummaryTable"),
      // redis keys
      redisRegisteredOfficerCountKey = "mdo_registered_officer_count",
      redisTotalOfficerCountKey = "mdo_total_officer_count",
      redisOrgNameKey = "mdo_name_by_org",
      redisTotalRegisteredOfficerCountKey = "mdo_total_registered_officer_count",
      redisTotalOrgCountKey = "mdo_total_org_count",
      redisExpectedUserCompetencyCount = "dashboard_expected_user_competency_count",
      redisDeclaredUserCompetencyCount = "dashboard_declared_user_competency_count",
      redisUserCompetencyDeclarationRate = "dashboard_user_competency_declaration_rate",
      redisOrgCompetencyDeclarationRate = "dashboard_org_competency_declaration_rate",
      redisUserCompetencyGapCount = "dashboard_user_competency_gap_count",
      redisUserCourseEnrollmentCount = "dashboard_user_course_enrollment_count",
      redisUserCompetencyGapEnrollmentRate = "dashboard_user_competency_gap_enrollment_rate",
      redisOrgCompetencyGapEnrollmentRate = "dashboard_org_competency_gap_enrollment_rate",
      redisUserCourseCompletionCount = "dashboard_user_course_completion_count",
      redisUserCompetencyGapClosedCount = "dashboard_user_competency_gap_closed_count",
      redisUserCompetencyGapClosedRate = "dashboard_user_competency_gap_closed_rate",
      redisOrgCompetencyGapClosedRate = "dashboard_org_competency_gap_closed_rate"
    )
  }

  def elasticSearchCourseProgramDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val query = """{"_source":["identifier","name","primaryCategory","status","reviewStatus","channel","duration","leafNodesCount"],"query":{"bool":{"should":[{"match":{"primaryCategory.raw":"Course"}},{"match":{"primaryCategory.raw":"Program"}}]}}}"""
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel", "duration", "leafNodesCount")
    elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields)
  }

  def elasticSearchLiveResourceDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val query = """{"_source":["identifier","name","primaryCategory","status","reviewStatus","channel"],"query":{"bool":{"must":[{"match":{"status.raw":"Live"}},{"match":{"primaryCategory.raw":"Learning Resource"}}]}}}"""
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel")
    elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields)
  }

  def fracCompetencyAPI(host: String): String = {
    val url = s"https://${host}/graphql"
    val requestBody = """{"operationName":"filterCompetencies","variables":{"cod":[],"competencyType":[],"competencyArea":[],"competencySector":[]},"query":"query filterCompetencies($cod: [String], $competencyType: [String], $competencyArea: [String], $competencySector: [String]) {\n  getAllCompetencies(\n    cod: $cod\n    competencyType: $competencyType\n    competencyArea: $competencyArea\n    competencySector: $competencySector\n  ) {\n    name\n    id\n    description\n    status\n    source\n    additionalProperties {\n      cod\n      competencyType\n      competencyArea\n      competencySector\n      __typename\n    }\n    __typename\n  }\n}\n"}"""
    api("POST", url, requestBody)
  }

  def fracCompetencyDFOption(host: String)(implicit spark: SparkSession): Option[DataFrame] = {
    var result = fracCompetencyAPI(host)
    result = result.trim()
    // return empty data frame if result is an empty string
    if (result == "") {
      println(s"ERROR: fracCompetencyAPI returned empty string")
      return None
    }
    val df = dataFrameFromJSONString(result)  // parse json string
    if (df.isEmpty) {
      println(s"ERROR: druidSQLAPI json parse result is empty")
      return None
    }
    // return empty data frame if there is an `errors` field in the json
    if (hasColumn(df, "errors")) {
      println(s"ERROR: fracCompetencyAPI returned error response, response=${result}")
      return None
    }
    // now that error handling is done, proceed with business as usual
    Some(df)
  }

  /**
   * completionPercentage   completionStatus    IDI status
   * NULL                   not-enrolled        not-started
   * 0.0                    enrolled            not-started
   * 0.0 < % < 10.0         started             enrolled
   * 10.0 <= % < 100.0      in-progress         in-progress
   * 100.0                  completed           completed
   * @param df data frame with completionPercentage column
   * @return df with completionStatus column
   */
  def withCompletionStatusColumn(df: DataFrame): DataFrame = {
    val caseExpression = "CASE WHEN ISNULL(completionPercentage) THEN 'not-enrolled' WHEN completionPercentage == 0.0 THEN 'enrolled' WHEN completionPercentage < 10.0 THEN 'started' WHEN completionPercentage < 100.0 THEN 'in-progress' ELSE 'completed' END"
    df.withColumn("completionStatus", expr(caseExpression))
  }

  /* Data processing functions */

  /**
   * org data from cassandra
   * @return DataFrame(orgID, orgName, orgStatus)
   */
  def orgDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val orgDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOrgTable)
      .select(
        col("id").alias("orgID"),
        col("orgname").alias("orgName"),
        col("status").alias("orgStatus")
      ).na.fill("", Seq("orgName"))

    show(orgDF, "Org DataFrame")

    orgDF
  }

  /**
   * user data from cassandra
   * @return DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus, userCreatedTimestamp, userUpdatedTimestamp)
   */
  def userDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    // userID, orgID, userStatus
    var userDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)
      .select(
        col("id").alias("userID"),
        col("firstname").alias("firstName"),
        col("lastname").alias("lastName"),
        col("maskedemail").alias("maskedEmail"),
        col("rootorgid").alias("userOrgID"),
        col("status").alias("userStatus"),
        col("createddate").alias("userCreatedTimestamp"),
        col("updateddate").alias("userUpdatedTimestamp")
      ).na.fill("", Seq("userOrgID"))

    userDF = userDF
      .withColumn("userCreatedTimestamp", to_timestamp(col("userCreatedTimestamp"), "yyyy-MM-dd HH:mm:ss:SSSZ"))
      .withColumn("userCreatedTimestamp", col("userCreatedTimestamp").cast("long"))
      .withColumn("userUpdatedTimestamp", to_timestamp(col("userUpdatedTimestamp"), "yyyy-MM-dd HH:mm:ss:SSSZ"))
      .withColumn("userUpdatedTimestamp", col("userUpdatedTimestamp").cast("long"))

    show(userDF, "User DataFrame")

    userDF
  }

  /**
   *
   * @param orgDF DataFrame(orgID, orgName, orgStatus)
   * @param userDF DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus, userCreatedTimestamp, userUpdatedTimestamp)
   * @return DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userCreatedTimestamp, userUpdatedTimestamp, userOrgID, userOrgName, userOrgStatus)
   */
  def userOrgDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {

    val joinOrgDF = orgDF.select(
      col("orgID").alias("userOrgID"),
      col("orgName").alias("userOrgName"),
      col("orgStatus").alias("userOrgStatus")
    )
    val userInfoDF = userDF.join(joinOrgDF, Seq("userOrgID"), "left")
    show(userInfoDF, "User Info DataFrame")

    userInfoDF
  }

  /**
   *
   * @return DataFrame(userID, role)
   */
  def roleDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val roleDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserRolesTable)
      .select(
        col("userid").alias("userID"),
        col("role").alias("role")
      )
    show(roleDF, "User Role DataFrame")

    roleDF
  }

  /**
   *
   * @param userOrgDF DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus)
   * @param roleDF DataFrame(userID, role)
   * @return DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   */
  def userOrgRoleDataFrame(userOrgDF: DataFrame, roleDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    // userID, userStatus, orgID, orgName, orgStatus, role
    val joinUserOrgDF = userOrgDF.select(
      col("userID"), col("userStatus"),
      col("userOrgID"), col("userOrgName"), col("userOrgStatus")
    )
    val userOrgRoleDF = joinUserOrgDF.join(roleDF, Seq("userID"), "left").where(expr("userStatus=1 AND userOrgStatus=1"))
    show(userOrgRoleDF)

    userOrgRoleDF
  }

  /**
   *
   * @param userOrgRoleDF DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   * @return DataFrame(role, count)
   */
  def roleCountDataFrame(userOrgRoleDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val roleCountDF = userOrgRoleDF.groupBy("role").agg(countDistinct("userID").alias("count"))
    show(roleCountDF)

    roleCountDF
  }

  /**
   *
   * @param userOrgRoleDF DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   * @return DataFrame(orgID, orgName, role, count)
   */
  def orgRoleCountDataFrame(userOrgRoleDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val orgRoleCount = userOrgRoleDF.groupBy("userOrgID", "role").agg(
      last("userOrgName").alias("orgName"),
      countDistinct("userID").alias("count")
    ).select(
      col("userOrgID").alias("orgID"),
      col("orgName"), col("role"), col("count")
    )
    show(orgRoleCount)

    orgRoleCount
  }

  /**
   *
   * @param orgDF DataFrame(orgID, orgName, orgStatus)
   * @param userDF DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus)
   * @return DataFrame(orgID, orgName, registeredCount, totalCount)
   */
  def orgUserCountDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val orgUserDF = orgDF.join(userDF.withColumnRenamed("userOrgID", "orgID").filter(col("orgID").isNotNull), Seq("orgID"), "left")
      .where(expr("userStatus=1 AND orgStatus=1"))
    show(orgUserDF, "Org User DataFrame")

    val orgUserCountDF = orgUserDF.groupBy("orgID", "orgName").agg(expr("count(userID)").alias("registeredCount"))
      .withColumn("totalCount", lit(10000))
    show(orgUserCountDF, "Org User Count DataFrame")

    orgUserCountDF
  }

  /**
   *
   * @param orgUserCountDF DataFrame(orgID, orgName, registeredCount, totalCount)
   * @return registered user count map, total  user count map, and orgID-orgName map
   */
  def getOrgUserMaps(orgUserCountDF: DataFrame): (util.Map[String, String], util.Map[String, String], util.Map[String, String]) = {
    val orgRegisteredUserCountMap = new util.HashMap[String, String]()
    val orgTotalUserCountMap = new util.HashMap[String, String]()
    val orgNameMap = new util.HashMap[String, String]()

    orgUserCountDF.collect().foreach(row => {
      val orgID = row.getAs[String]("orgID")
      orgRegisteredUserCountMap.put(orgID, row.getAs[Long]("registeredCount").toString)
      orgTotalUserCountMap.put(orgID, row.getAs[Long]("totalCount").toString)
      orgNameMap.put(orgID, row.getAs[String]("orgName"))
    })

    (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap)
  }

  /**
   * All courses/programs from elastic search api
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID)
   */
  def allCourseProgramESDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = elasticSearchCourseProgramDataFrame()

    // now that error handling is done, proceed with business as usual
    df = df.select(
      col("identifier").alias("courseID"),
      col("primaryCategory").alias("category"),
      col("name").alias("courseName"),
      col("status").alias("courseStatus"),
      col("reviewStatus").alias("courseReviewStatus"),
      col("channel").alias("courseOrgID")
      // col("duration").alias("courseDuration"),
      // col("leafNodesCount").alias("courseResourceCount")
    )
    df = df.dropDuplicates("courseID", "category")
    // df = df.na.fill(0.0, Seq("courseDuration")).na.fill(0, Seq("courseResourceCount"))

    show(df, "allCourseProgramESDataFrame")
    df
  }

  /**
   * Attach org info to course/program data
   * @param allCourseProgramESDF DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID)
   * @param orgDF DataFrame(orgID, orgName, orgStatus)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus)
   */
  def allCourseProgramDataFrame(allCourseProgramESDF: DataFrame, orgDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {

    val joinOrgDF = orgDF.select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )
    val df = allCourseProgramESDF.join(joinOrgDF, Seq("courseOrgID"), "left")

    show(df, "allCourseProgramDataFrame")
    df
  }

  /* schema definitions for courseDetailsDataFrame */
  val courseHierarchySchema: StructType = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("duration", StringType, nullable = true),
    StructField("leafNodesCount", IntegerType, nullable = true),
    StructField("competencies_v3", StringType, nullable = true)
  ))
  /**
   * course details with competencies json from cassandra dev_hierarchy_store:content_hierarchy
   * @param allCourseProgramDF Dataframe(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   */
  def allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val rawCourseDF = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraContentHierarchyTable)
      .select(col("identifier").alias("courseID"), col("hierarchy"))

    // inner join so that we only retain live courses
    var df = allCourseProgramDF.join(rawCourseDF, Seq("courseID"), "left")

    df = df.na.fill("{}", Seq("hierarchy"))
    df = df.withColumn("data", from_json(col("hierarchy"), courseHierarchySchema))
    df = df.select(
      col("courseID"), col("category"), col("courseName"), col("courseStatus"),
      col("courseReviewStatus"), col("courseOrgID"), col("courseOrgName"), col("courseOrgStatus"),

      col("data.duration").cast(FloatType).alias("courseDuration"),
      col("data.leafNodesCount").alias("courseResourceCount"),
      col("data.competencies_v3").alias("competenciesJson")
    )
    df = df.na.fill(0.0, Seq("courseDuration")).na.fill(0, Seq("courseResourceCount"))

    show(df, "allCourseProgramDetailsWithCompetenciesJsonDataFrame() = (courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)")
    df
  }


  /**
   * Live resources from elastic search api
   * @return DataFrame(resourceID, resourceStatus, resourceReviewStatus, resourceOrgID)
   */
  val allResourceSchema: StructType = StructType(Seq(
    StructField("resourceID",  StringType, nullable = true),
    StructField("resourceStatus",  StringType, nullable = true),
    StructField("resourceReviewStatus",  StringType, nullable = true),
    StructField("resourceOrgID",  StringType, nullable = true)
  ))
  def liveResourceDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = elasticSearchLiveResourceDataFrame()

    // now that error handling is done, proceed with business as usual
    df = df.select(
      col("identifier").alias("resourceID"),
      col("status").alias("resourceStatus"),
      col("reviewStatus").alias("resourceReviewStatus"),
      col("channel").alias("resourceOrgID")
    )
    df = df.dropDuplicates("resourceID")

    show(df)
    df
  }

  // only live course ids
  val liveCourseSchema: StructType = StructType(Seq(
    StructField("id",  StringType, nullable = true)
  ))
  def liveCourseDataFrame(allCourseProgramDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val df = allCourseProgramDF.where(expr("category='Course' and courseStatus='Live'")).select(col("courseID").alias("id")).distinct()

    show(df)
    df
  }

  /**
   * course details without competencies json
   * @param allCourseProgramDetailsWithCompDF course details with competencies json
   *                                          DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                          courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   */
  def allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF: DataFrame): DataFrame = {
    val df = allCourseProgramDetailsWithCompDF.drop("competenciesJson")

    show(df)
    df
  }

  /* schema definitions for courseCompetencyDataFrame */
  val courseCompetenciesSchema: ArrayType = ArrayType(StructType(Seq(
    StructField("id",  StringType, nullable = true),
    StructField("name",  StringType, nullable = true),
    // StructField("description",  StringType, nullable = true),
    // StructField("source",  StringType, nullable = true),
    StructField("competencyType",  StringType, nullable = true),
    // StructField("competencyArea",  StringType, nullable = true),
    // StructField("selectedLevelId",  StringType, nullable = true),
    // StructField("selectedLevelName",  StringType, nullable = true),
    // StructField("selectedLevelSource",  StringType, nullable = true),
    StructField("selectedLevelLevel",  StringType, nullable = true)
    //StructField("selectedLevelDescription",  StringType, nullable = true)
  )))
  /**
   * course competency mapping data from cassandra dev_hierarchy_store:content_hierarchy
   * @param allCourseProgramDetailsWithCompDF course details with competencies json
   *                                          DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                          courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)
   */
  def allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = allCourseProgramDetailsWithCompDF.filter(col("competenciesJson").isNotNull)
    df = df.withColumn("competencies", from_json(col("competenciesJson"), courseCompetenciesSchema))

    df = df.select(
      col("courseID"), col("category"), col("courseName"), col("courseStatus"),
      col("courseReviewStatus"), col("courseOrgID"), col("courseOrgName"), col("courseOrgStatus"),
      col("courseDuration"), col("courseResourceCount"),
      explode_outer(col("competencies")).alias("competency")
    )
    df = df.filter(col("competency").isNotNull)
    df = df.withColumn("competencyLevel", expr("TRIM(competency.selectedLevelLevel)"))
    df = df.withColumn("competencyLevel",
      expr("IF(competencyLevel RLIKE '[0-9]+', CAST(REGEXP_EXTRACT(competencyLevel, '[0-9]+', 0) AS INTEGER), 1)"))
    df = df.select(
      col("courseID"), col("category"), col("courseName"), col("courseStatus"),
      col("courseReviewStatus"), col("courseOrgID"), col("courseOrgName"), col("courseOrgStatus"),
      col("courseDuration"), col("courseResourceCount"),
      col("competency.id").alias("competencyID"),
      col("competency.name").alias("competencyName"),
      col("competency.competencyType").alias("competencyType"),
      col("competencyLevel")
    )

    show(df, "allCourseProgramCompetencyDataFrame (courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)")
    df
  }

  /**
   *
   * @param allCourseProgramCompetencyDF DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *                                     courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)
   * @return DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         competencyID, competencyLevel)
   */
  def liveCourseCompetencyDataFrame(allCourseProgramCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val df = allCourseProgramCompetencyDF.where(expr("courseStatus='Live' AND category='Course'"))
      .select("courseID", "courseName", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration",
        "courseResourceCount", "competencyID", "competencyLevel")

    show(df, "liveCourseCompetencyDataFrame (courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyLevel)")
    df
  }


  /**
   * data frame of course rating summary
   * @return DataFrame(courseID, categoryLower, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
   */
  def courseRatingSummaryDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingSummaryTable)
      .where(expr("total_number_of_ratings > 0"))
      .withColumn("ratingAverage", expr("sum_of_total_ratings / total_number_of_ratings"))
      .select(
        col("activityid").alias("courseID"),
        col("activitytype").alias("categoryLower"),
        col("sum_of_total_ratings").alias("ratingSum"),
        col("total_number_of_ratings").alias("ratingCount"),
        col("ratingAverage"),
        col("totalcount1stars").alias("count1Star"),
        col("totalcount2stars").alias("count2Star"),
        col("totalcount3stars").alias("count3Star"),
        col("totalcount4stars").alias("count4Star"),
        col("totalcount5stars").alias("count5Star")
      )
    show(df, "courseRatingSummaryDataFrame before duplicate drop")

    df = df.withColumn("categoryLower", lower(col("categoryLower")))
      .dropDuplicates("courseID", "categoryLower")

    show(df, "courseRatingSummaryDataFrame")
    df
  }

  /**
   * add course rating columns to course detail data-frame
   * @param allCourseProgramDetailsDF course details data frame -
   *                                  DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                  courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   * @param courseRatingDF course rating summary data frame -
   *                       DataFrame(courseID, ratingSum, ratingCount, ratingAverage,
   *                       count1Star, count2Star, count3Star, count4Star, count5Star)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus, courseDuration, courseResourceCount, ratingSum, ratingCount, ratingAverage, count1Star,
   *         count2Star, count3Star, count4Star, count5Star)
   */
  def allCourseProgramDetailsWithRatingDataFrame(allCourseProgramDetailsDF: DataFrame, courseRatingDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val df = allCourseProgramDetailsDF.join(courseRatingDF.drop("categoryLower"), Seq("courseID"), "left")

    show(df)
    df
  }

  /**
   *
   * @return DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus)
   */
  def userCourseProgramCompletionDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val df = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserEnrolmentsTable)
      .where(expr("active=true"))
      .withColumn("courseCompletedTimestamp", col("completedon").cast("long"))
      .withColumn("courseEnrolledTimestamp", col("enrolled_date").cast("long"))
      .withColumn("lastContentAccessTimestamp", col("lastcontentaccesstime").cast("long"))
      .select(
        col("userid").alias("userID"),
        col("courseid").alias("courseID"),
        col("batchid").alias("batchID"),
        col("progress").alias("courseProgress"),
        col("status").alias("dbCompletionStatus"),
        col("courseCompletedTimestamp"),
        col("courseEnrolledTimestamp"),
        col("lastContentAccessTimestamp")
      ).na.fill(0, Seq("courseProgress"))

    show(df)
    df
  }

  /**
   * get course completion data with details attached
   * @param userCourseProgramCompletionDF  DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus)
   * @param allCourseProgramDetailsDF course details data frame -
   *                                  DataFrame(courseID, category, courseName, courseStatus,
   *                                  courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   * @param userOrgDF DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus)
   * @return DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp,
   *         courseProgress, dbCompletionStatus, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *         courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, firstName, lastName, maskedEmail, userStatus,
   *         userOrgID, userOrgName, userOrgStatus, completionPercentage, completionStatus)
   */
  def allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF: DataFrame, allCourseProgramDetailsDF: DataFrame, userOrgDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    // userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount
    var df = userCourseProgramCompletionDF.join(allCourseProgramDetailsDF, Seq("courseID"), "left")
    show(df, "userAllCourseProgramCompletionDataFrame s=1")

    df = df.join(userOrgDF, Seq("userID"), "left")
      .select("userID", "courseID", "batchID", "courseCompletedTimestamp", "courseEnrolledTimestamp",
        "lastContentAccessTimestamp", "courseProgress", "dbCompletionStatus", "category", "courseName",
        "courseStatus", "courseReviewStatus", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration",
        "courseResourceCount", "firstName", "lastName", "maskedEmail", "userStatus", "userOrgID", "userOrgName", "userOrgStatus")
    df = df.withColumn("completionPercentage", expr("CASE WHEN courseProgress=0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"))
    df = withCompletionStatusColumn(df)

    show(df, "allCourseProgramCompletionWithDetailsDataFrame")

    df
  }

  /**
   *
   * @param allCourseProgramCompletionWithDetailsDF DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *         courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *         completionStatus)
   * @return DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *         courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *         completionStatus)
   */
  def liveRetiredCourseCompletionWithDetailsDataFrame(allCourseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val df = allCourseProgramCompletionWithDetailsDF.where(expr("courseStatus in ('Live', 'Retired') AND category='Course'"))
    show(df, "liveRetiredCourseCompletionWithDetailsDataFrame")
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them from druid
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   */
  val expectedCompetencySchema: StructType = StructType(Seq(
    StructField("orgID",  StringType, nullable = true),
    StructField("workOrderID",  StringType, nullable = true),
    StructField("userID",  StringType, nullable = true),
    StructField("competencyID",  StringType, nullable = true),
    StructField("expectedCompetencyLevel",  IntegerType, nullable = true)
  ))
  def expectedCompetencyDataFrame()(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
    val query = """SELECT edata_cb_data_deptId AS orgID, edata_cb_data_wa_id AS workOrderID, edata_cb_data_wa_userId AS userID, edata_cb_data_wa_competency_id AS competencyID, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS expectedCompetencyLevel FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (df == null) return emptySchemaDataFrame(expectedCompetencySchema)

    df = df.filter(col("competencyID").isNotNull && col("expectedCompetencyLevel").notEqual(0))
      .withColumn("expectedCompetencyLevel", expr("CAST(expectedCompetencyLevel as INTEGER)"))  // Important to cast as integer otherwise a cast will fail later on
      .filter(col("expectedCompetencyLevel").isNotNull && col("expectedCompetencyLevel").notEqual(0))

    show(df)
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them, including live course count
   * @param expectedCompetencyDF expected competency data frame -
   *                             DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName,
   *                               courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyLevel)
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel, liveCourseCount)
   */
  def expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF: DataFrame, liveCourseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
    // live course count DF
    val liveCourseCountDF = expectedCompetencyDF.join(liveCourseCompetencyDF, Seq("competencyID"), "left")
      .where(expr("expectedCompetencyLevel <= competencyLevel"))
      .groupBy("orgID", "workOrderID", "userID", "competencyID", "expectedCompetencyLevel")
      .agg(countDistinct("courseID").alias("liveCourseCount"))

    val df = expectedCompetencyDF.join(liveCourseCountDF, Seq("orgID", "workOrderID", "userID", "competencyID", "expectedCompetencyLevel"), "left")
      .na.fill(0, Seq("liveCourseCount"))

    show(df)
    df
  }

  /* schema definitions for declaredCompetencyDataFrame */
  val profileCompetencySchema: StructType = StructType(Seq(
    StructField("id",  StringType, nullable = true),
    StructField("name",  StringType, nullable = true),
    StructField("status",  StringType, nullable = true),
    StructField("competencyType",  StringType, nullable = true),
    StructField("competencySelfAttestedLevel",  StringType, nullable = true), // this is sometimes an int other times a string
    StructField("competencySelfAttestedLevelValue",  StringType, nullable = true)
  ))
  val profileDetailsSchema: StructType = StructType(Seq(
    StructField("competencies", ArrayType(profileCompetencySchema), nullable = true)
  ))

  object CompLevelParser extends Serializable {

    val competencyLevelPattern: Regex = ".*[Ll]evel[ ]+?([0-9]+).*".r
    /**
     * match string against level pattern and return level or zero
     * @param s string to parse
     * @return level or zero
     */
    def parseCompetencyLevelString(s: String): Int = {
      s match {
        case competencyLevelPattern(level) => level.toInt
        case _ => 0
      }
    }
    /**
     * get competency level from string value
     * @param levelString level string
     * @return level value as int
     */
    def getCompetencyLevel(levelString: String): Int = {
      intOrZero(levelString) match {
        case 0 => parseCompetencyLevelString(levelString)
        case default => default
      }
    }

    /**
     * spark udf to infer competency level value, returns 1 if no value could be inferred
     * @param csaLevel value of competencySelfAttestedLevel column
     * @param csaLevelValue value of competencySelfAttestedLevelValue column
     * @return level value as int
     */
    def compLevelParser(csaLevel: String, csaLevelValue: String): Int = {
      for (levelString <- Seq(csaLevel, csaLevelValue)) {
        val level = getCompetencyLevel(levelString)
        if (level != 0) return level
      }
      1 // return 1 as default
    }
    val compLevelParserUdf: UserDefinedFunction = udf(compLevelParser _)
  }

  /**
   * User's declared competency data from cassandra sunbird:user
   * @return DataFrame(userID, competencyID, declaredCompetencyLevel)
   */
  def declaredCompetencyDataFrame()(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
    val userdata = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)

    // select id and profile details column where profile details are available
    var df = userdata.where(col("profiledetails").isNotNull).select("id", "profiledetails")
    // json parse profile details
    df = df.withColumn("profile", from_json(col("profiledetails"), profileDetailsSchema))
    // explode competencies
    df = df.select(col("id"), explode_outer(col("profile.competencies")).alias("competency"))
    // filter out where competency or competency id not present
    df = df.where(col("competency").isNotNull && col("competency.id").isNotNull)

    // use udf for competency level parsing, as the schema for competency level is broken
    df = df.withColumn("declaredCompetencyLevel",
      CompLevelParser.compLevelParserUdf(col("competency.competencySelfAttestedLevel"), col("competency.competencySelfAttestedLevelValue"))
    ).na.fill(1, Seq("declaredCompetencyLevel"))  // if competency is listed without a level assume level 1

    // select useful columns
    df = df.select(
      col("id").alias("userID"),
      col("competency.id").alias("competencyID"),
      col("declaredCompetencyLevel")
    )

    show(df, "declaredCompetencyDataFrame [userID, competencyID, declaredCompetencyLevel]")
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api
   * @return DataFrame(competencyID, competencyName, competencyStatus)
   */
  val fracCompetencySchema: StructType = StructType(Seq(
    StructField("competencyID",  StringType, nullable = true),
    StructField("competencyName",  StringType, nullable = true),
    StructField("competencyStatus",  StringType, nullable = true)
  ))
  def fracCompetencyDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = fracCompetencyDFOption(conf.fracBackendHost).orNull
    if (df == null) return emptySchemaDataFrame(fracCompetencySchema)

    df = df
      .select(explode_outer(col("data.getAllCompetencies")).alias("competency"))
      .select(
        col("competency.id").alias("competencyID"),
        col("competency.name").alias("competencyName"),
        col("competency.status").alias("competencyStatus")
      )
      .where(expr("LOWER(competencyStatus) = 'verified'"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including live course count
   * @param fracCompetencyDF frac competency data frame -
   *                         DataFrame(competencyID, competencyName, competencyStatus)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration,
   *                               courseResourceCount, competencyID, competencyLevel)
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
   */
  def fracCompetencyWithCourseCountDataFrame(fracCompetencyDF: DataFrame, liveCourseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
    // live course count DF
    val liveCourseCountDF = fracCompetencyDF.join(liveCourseCompetencyDF, Seq("competencyID"), "left")
      .filter(col("courseID").isNotNull)
      .groupBy("competencyID", "competencyName", "competencyStatus")
      .agg(countDistinct("courseID").alias("liveCourseCount"))

    val df = fracCompetencyDF.join(liveCourseCountDF, Seq("competencyID", "competencyName", "competencyStatus"), "left")
      .na.fill(0, Seq("liveCourseCount"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including officer count
   * @param fracCompetencyWithCourseCountDF frac competency data frame with live course count
   * @param expectedCompetencyDF expected competency data frame
   * @param declaredCompetencyDF declared  competency data frame
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount, officerCountExpected, officerCountDeclared)
   */
  def fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF: DataFrame, expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
    // fracCompetencyWithCourseCountDF = DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
    // expectedCompetencyDF = DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
    // declaredCompetencyDF = DataFrame(userID, competencyID, declaredCompetencyLevel)

    // add expected officer count
    val fcExpectedCountDF = fracCompetencyWithCourseCountDF.join(expectedCompetencyDF, Seq("competencyID"), "leftouter")
      .groupBy("competencyID", "competencyName", "competencyStatus")
      .agg(countDistinct("userID").alias("officerCountExpected"))

    // add declared officer count
    val fcExpectedDeclaredCountDF = fcExpectedCountDF.join(declaredCompetencyDF, Seq("competencyID"), "leftouter")
      .groupBy("competencyID", "competencyName", "competencyStatus", "officerCountExpected")
      .agg(countDistinct("userID").alias("officerCountDeclared"))

    val df = fracCompetencyWithCourseCountDF.join(fcExpectedDeclaredCountDF, Seq("competencyID", "competencyName", "competencyStatus"), "left")

    show(df)
    df
  }

  /**
   * Calculates user's competency gaps
   * @param expectedCompetencyDF expected competency data frame -
   *                             DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   * @param declaredCompetencyDF declared competency data frame -
   *                             DataFrame(userID, competencyID, declaredCompetencyLevel)
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap)
   */
  def competencyGapDataFrame(expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    var df = expectedCompetencyDF.join(declaredCompetencyDF, Seq("competencyID", "userID"), "left")
    df = df.na.fill(0, Seq("declaredCompetencyLevel"))  // if null values created during join fill with 0
    df = df.groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(
        max("expectedCompetencyLevel").alias("expectedCompetencyLevel"),  // in-case of multiple entries, take max
        max("declaredCompetencyLevel").alias("declaredCompetencyLevel")  // in-case of multiple entries, take max
      )
    df = df.withColumn("competencyGap", expr("expectedCompetencyLevel - declaredCompetencyLevel"))

    show(df)
    df
  }

  /**
   * add course data to competency gap data, add user course completion info on top, calculate user competency gap status
   *
   * @param competencyGapDF competency gap data frame -
   *                        DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *                               competencyID, competencyLevel)
   * @param allCourseProgramCompletionWithDetailsDF user course completion data frame -
   *                                                DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *                                                courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *                                                firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *                                                completionStatus)
   *
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, completionPercentage, completionStatus)
   */
  def competencyGapCompletionDataFrame(competencyGapDF: DataFrame, liveCourseCompetencyDF: DataFrame, allCourseProgramCompletionWithDetailsDF: DataFrame): DataFrame = {

    // userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, courseID,
    // courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyLevel
    val cgCourseDF = competencyGapDF.filter("competencyGap > 0")
      .join(liveCourseCompetencyDF, Seq("competencyID"), "leftouter")
      .filter("expectedCompetencyLevel >= competencyLevel")

    // drop duplicate columns before join
    val courseCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDF.drop("courseName", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration", "courseResourceCount")

    // userID, competencyID, orgID, workOrderID, completionPercentage
    val gapCourseUserStatus = cgCourseDF.join(courseCompletionWithDetailsDF, Seq("userID", "courseID"), "left")
      .groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(max(col("completionPercentage")).alias("completionPercentage"))
      .withColumn("completionPercentage", expr("IF(ISNULL(completionPercentage), 0.0, completionPercentage)"))

    var df = competencyGapDF.join(gapCourseUserStatus, Seq("userID", "competencyID", "orgID", "workOrderID"), "left")

    df = withCompletionStatusColumn(df)

    show(df)
    df
  }

}