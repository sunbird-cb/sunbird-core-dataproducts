package org.ekstep.analytics.dashboard.report.click

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{IBatchModelTemplate, _}

import java.io.Serializable
import java.util

case class CSConfig(
                     debug: String, validation: String,
                     // kafka connection config
                     broker: String, compression: String,
                     // redis connection config
                     redisHost: String, redisPort: Int, redisDB: Int,
                     // other hosts connection config
                     sparkCassandraConnectionHost: String, sparkDruidRouterHost: String,
                     sparkElasticsearchConnectionHost: String, fracBackendHost: String,
                     // cassandra key spaces
                     cassandraUserKeyspace: String,
                     cassandraCourseKeyspace: String, cassandraHierarchyStoreKeyspace: String,
                     // cassandra table details
                     cassandraUserTable: String, cassandraUserRolesTable: String, cassandraOrgTable: String,
                     cassandraUserEnrolmentsTable: String, cassandraContentHierarchyTable: String,
                     cassandraRatingSummaryTable: String
                   ) extends DashboardConfig

/**
 * Model for processing competency metrics
 */
object CourseMetricsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.CourseMetricsModel"
  override def name() = "CourseMetricsModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCompetencyMetricsData(timestamp, config)
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
  def processCompetencyMetricsData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: CSConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // obtain and save user org data
    val orgDF = orgDataFrame()
    val userDF = userDataFrame()
    val userOrgDF = userOrgDataFrame(orgDF, userDF)
    // validate userDF and userOrgDF counts
    validate({userDF.count()}, {userOrgDF.count()}, "userDF.count() should equal userOrgDF.count()")
    // kafkaDispatch(withTimestamp(userOrgDF, timestamp), conf.userOrgTopic)

    // userDF = userDF.drop("userCreatedTimestamp", "userUpdatedTimestamp")
    // userOrgDF = userOrgDF.drop("userCreatedTimestamp", "userUpdatedTimestamp")

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
//    validate(
//      {courseRatingDF.where(expr("categoryLower IN ('course', 'program') AND ratingSum > 0")).count()},
//      {allCourseProgramDetailsWithRatingDF.where(expr("LOWER(category) IN ('course', 'program') AND ratingSum > 0")).count()},
//      "number of ratings in cassandra table for courses and programs with ratingSum > 0 should equal those in final druid datasource")
//    // validate rating data, sanity check
//    Seq(1, 2, 3, 4, 5).foreach(i => {
//      validate(
//        {courseRatingDF.where(expr(s"categoryLower IN ('course', 'program') AND ratingAverage <= ${i}")).count()},
//        {allCourseProgramDetailsWithRatingDF.where(expr(s"LOWER(category) IN ('course', 'program') AND ratingAverage <= ${i}")).count()},
//        s"Rating data row count for courses and programs should equal final DF for ratingAverage <= ${i}"
//      )
//    })

    // click by id
    // click by provider
    // click by course duration
    // click by course rating
    // click by competencies
    // click by enrollments
    // click by completions
    // click by in-progress

    val readLoc = "/home/analytics/click-stream-data/"
    val loc = "/home/analytics/click-stream-data/gen/"

    // object_id, object_type, clicks
    val clickDF = spark.read.format("csv").option("header", "true")
      .load(s"${readLoc}clicks-by-content-id.csv")
      .select(
        col("object_id").alias("courseID"),
        col("object_type").alias("category"),
        col("clicks")
      )
    show(clickDF, "clickDF")
    val clickWithDetailsDF = clickDF.join(allCourseProgramDetailsWithRatingDF, Seq("courseID", "category"), "left")
    show(clickWithDetailsDF, "clickWithDetailsDF")

    // click by id
    val clickByIDDF = clickWithDetailsDF.select("courseID", "courseName", "category", "clicks")
      .orderBy(desc("clicks"))
    show(clickByIDDF, "clickByIDDF")
    csvWrite(clickByIDDF, s"${loc}clicks-by-content-name.csv")

    // click by provider
    val clickByProviderDF = clickWithDetailsDF.groupBy("courseOrgID", "courseOrgName")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy(desc("clicks"))
    show(clickByProviderDF, "clickByProviderDF")
    csvWrite(clickByProviderDF, s"${loc}clicks-by-provider.csv")

    // click by course duration
    val clickByDuration = clickWithDetailsDF.withColumn("durationFloorHrs", expr("CAST(FLOOR(courseDuration / 3600) AS INTEGER)"))
      .groupBy("durationFloorHrs")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy("durationFloorHrs")
    show(clickByDuration, "clickByDuration")
    csvWrite(clickByDuration, s"${loc}clicks-by-duration.csv")

    // click by course rating
    val clickByRating = clickWithDetailsDF.withColumn("ratingFloor", expr("CAST(FLOOR(ratingAverage) AS INTEGER)"))
      .groupBy("ratingFloor")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy("ratingFloor")
    show(clickByRating, "clickByRating")
    csvWrite(clickByRating, s"${loc}clicks-by-rating.csv")

    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)
    val clickWithCompetencyDF = clickDF.join(allCourseProgramCompetencyDF, Seq("courseID", "category"), "left")

    // click by competencies
    val clickByCompetencyDF = clickWithCompetencyDF.groupBy("competencyID", "competencyName")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy(desc("clicks"))
    show(clickByCompetencyDF, "clickByCompetencyDF")
    csvWrite(clickByCompetencyDF, s"${loc}clicks-by-comp.csv")

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame()
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)
    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")

    val contentUserStatusCountDF = allCourseProgramCompletionWithDetailsDF
      .where(expr("courseStatus IN ('Live', 'Retired')"))
      .groupBy("courseID", "courseName", "category")
      .agg(
        expr("COUNT(courseID)").alias("countEnrolled"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("countInProgress"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("countCompleted")
      )
    show(contentUserStatusCountDF, "contentUserStatusCountDF")
    val clickWithProgressCountsDF = clickDF.join(contentUserStatusCountDF, Seq("courseID", "category"), "left")
    show(clickWithProgressCountsDF, "clickWithProgressCountsDF")

    val clickByEnroll = bucketGroupBy(clickWithProgressCountsDF, "countEnrolled", "clicks")
    show(clickByEnroll, "clickByEnroll")
    csvWrite(clickByEnroll,s"${loc}clicks-by-enroll.csv")

    val clickByInProgress = bucketGroupBy(clickWithProgressCountsDF, "countInProgress", "clicks")
    show(clickByInProgress, "clickByInProgress")
    csvWrite(clickByInProgress, s"${loc}clicks-by-in-progress.csv")

    val clickByCompleted = bucketGroupBy(clickWithProgressCountsDF, "countCompleted", "clicks")
    show(clickByCompleted, "clickByCompleted")
    csvWrite(clickByCompleted,s"${loc}clicks-by-completed.csv")

    closeRedisConnect()
  }

  def csvWrite(df: DataFrame, path: String): Unit = {
    df
      .coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save(path)
  }

  def bucketGroupBy(df: DataFrame, bucketCol: String, groupCol: String): DataFrame = {
    val maxVal = df.agg(expr(s"MAX(${bucketCol})")).head().getLong(0)  // 12345
    val orderMaxVal = Math.log10(maxVal).floor.toLong  // 4
    val order = Math.pow(10, orderMaxVal - 1).toLong  // 1000

    df.withColumn(s"${bucketCol}Floor", expr(s"CAST((FLOOR(${bucketCol} / ${order}) * ${order}) AS LONG)"))
      .groupBy(s"${bucketCol}Floor")
      .agg(expr(s"SUM(${groupCol})").alias(s"${groupCol}"))
      .orderBy(s"${bucketCol}Floor")
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


  def parseConfig(config: Map[String, AnyRef]): CSConfig = {
    CSConfig(
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
      cassandraRatingSummaryTable = getConfigModelParam(config, "cassandraRatingSummaryTable")
    )
  }

  def elasticSearchCourseProgramDataFrame()(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
    val query = """{"_source":["identifier","name","primaryCategory","status","reviewStatus","channel","duration","leafNodesCount"],"query":{"bool":{"should":[{"match":{"primaryCategory.raw":"Course"}},{"match":{"primaryCategory.raw":"CuratedCollections"}},{"match":{"primaryCategory.raw":"Program"}}]}}}"""
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel", "duration", "leafNodesCount")
    elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields)
  }

  /* Data processing functions */

  /**
   * org data from cassandra
   * @return DataFrame(orgID, orgName, orgStatus)
   */
  def orgDataFrame()(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def userDataFrame()(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def userOrgDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {

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
  def roleDataFrame()(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def userOrgRoleDataFrame(userOrgDF: DataFrame, roleDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def roleCountDataFrame(userOrgRoleDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
    val roleCountDF = userOrgRoleDF.groupBy("role").agg(countDistinct("userID").alias("count"))
    show(roleCountDF)

    roleCountDF
  }

  /**
   *
   * @param userOrgRoleDF DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   * @return DataFrame(orgID, orgName, role, count)
   */
  def orgRoleCountDataFrame(userOrgRoleDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def orgUserCountDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def allCourseProgramESDataFrame()(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def allCourseProgramDataFrame(allCourseProgramESDF: DataFrame, orgDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {

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
  def allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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

  // only live course ids
  val liveCourseSchema: StructType = StructType(Seq(
    StructField("id",  StringType, nullable = true)
  ))
  def liveCourseDataFrame(allCourseProgramDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def liveCourseCompetencyDataFrame(allCourseProgramCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def courseRatingSummaryDataFrame()(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
  def allCourseProgramDetailsWithRatingDataFrame(allCourseProgramDetailsDF: DataFrame, courseRatingDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
    val df = allCourseProgramDetailsDF.withColumn("categoryLower", expr("LOWER(category)"))
      .join(courseRatingDF, Seq("courseID", "categoryLower"), "left")

    show(df)
    df
  }

  /**
   *
   * @return DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus)
   */
  def userCourseProgramCompletionDataFrame()(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
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
   *         userOrgID, userOrgName, userOrgStatus, completionPercentage)
   */
  def allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF: DataFrame, allCourseProgramDetailsDF: DataFrame, userOrgDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
    // userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount
    var df = userCourseProgramCompletionDF.join(allCourseProgramDetailsDF, Seq("courseID"), "left")
    show(df, "userAllCourseProgramCompletionDataFrame s=1")

    df = df.join(userOrgDF, Seq("userID"), "left")
      .select("userID", "courseID", "batchID", "courseCompletedTimestamp", "courseEnrolledTimestamp",
        "lastContentAccessTimestamp", "courseProgress", "dbCompletionStatus", "category", "courseName",
        "courseStatus", "courseReviewStatus", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration",
        "courseResourceCount", "firstName", "lastName", "maskedEmail", "userStatus", "userOrgID", "userOrgName", "userOrgStatus")
    df = df.withColumn("completionPercentage", expr("CASE WHEN courseProgress=0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"))

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
  def liveRetiredCourseCompletionWithDetailsDataFrame(allCourseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, conf: CSConfig): DataFrame = {
    val df = allCourseProgramCompletionWithDetailsDF.where(expr("courseStatus in ('Live', 'Retired') AND category='Course'"))
    show(df, "liveRetiredCourseCompletionWithDetailsDataFrame")
    df
  }

}