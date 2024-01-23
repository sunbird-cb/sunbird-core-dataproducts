package org.ekstep.analytics.dashboard.report.commsconsole

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}


object CommsReportModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.commsconsole.CommsReportModel"
  override def name() = "CommsReportModel"
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
    processCommsConsoleReport(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  /**
   * Post processing on the algorithm output.
   */
  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processCommsConsoleReport(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val today = getDate()

    val dateFormat1 = "dd/MM/yyyy"
    val dateFormat2 = "yyyy-MM-dd"
    val bkEmailSuffix = conf.commsConsolePrarambhEmailSuffix
    val numOfDays = -conf.commsConsoleNumDaysToConsider
    val numOfTopLearners = conf.commsConsoleNumTopLearnersToConsider
    val currentDate = current_date()
    val dateNDaysAgo = date_add(currentDate, numOfDays)
    val lastUpdatedOn = date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a")
    val commsConsoleReportPath = s"${conf.commsConsoleReportPath}/${today}"

    val orgDF = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.orgHierarchyReportPath}/${today}-warehouse")
      .withColumn("department", when(col("ministry").isNotNull && col("department").isNull, col("mdo_name")).otherwise(col("department")))
      .withColumn("ministry", when(col("ministry").isNull && col("department").isNull, col("mdo_name")).otherwise(col("ministry")))
      .select("mdo_id", "ministry", "department", "organization")

    val userDF = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.userReportPath}/${today}-warehouse")
      .withColumn("registrationDate", to_date(col("user_registration_date"), dateFormat1))
      .select("user_id", "mdo_id", "full_name", "email", "phone_number", "roles", "registrationDate", "tag", "user_registration_date")
      .join(orgDF, Seq("mdo_id"), "left")

    val rawEnrollmentsDF = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.userEnrolmentReportPath}/${today}-warehouse")
      .withColumn("completionDate", to_date(col("completed_on"), dateFormat2))
    val enrollmentsDF = rawEnrollmentsDF
      .join(userDF, Seq("user_id"), "left")

    val mdoUserCountsDF = userDF.groupBy("mdo_id")
      .agg(
        count("*").alias("userCount")
      )

    val mdoCBPCompletionsDF = enrollmentsDF.filter(col("user_consumption_status") === "completed").groupBy("mdo_id")
      .agg(
        count("*").alias("completionCount")
      )
    val mdoAdminDF = userDF.filter(col("roles").contains("MDO_LEADER") || col("roles").contains("MDO_ADMIN"))
    val mdoCompletionRateDF = mdoCBPCompletionsDF.join(mdoUserCountsDF, Seq("mdo_id"), "left")
      .withColumn("completionPerUser", col("completionCount") / col("userCount"))
    val mdoCompletionRateWithAdminDetailsDF = mdoCompletionRateDF.join(mdoAdminDF, Seq("mdo_id"), "left")
     .orderBy(desc("completionPerUser"), desc("mdo_id"))
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("roles").alias("Role"),
        col("userCount").alias("Total_Users"),
        col("completionCount").alias("Total_Content_Completion"),
        col("completionPerUser").alias("Content_Completion_Per_User"),
        col("Last_Updated_On")
      )
    generateReportsWithoutPartition(mdoCompletionRateWithAdminDetailsDF, s"${commsConsoleReportPath}/topMdoCompletionRatio", "topMdoCompletionRatio")

    val usersWithEnrollments = enrollmentsDF.select("user_id").distinct()
    //users who have not been enrolled in any cbp
    val usersWithoutAnyEnrollments = userDF.select("user_id").distinct().except(usersWithEnrollments)
    val usersWithoutAnyEnrollmentsWithUserDetailsDF = usersWithoutAnyEnrollments.join(userDF, Seq("user_id"), "left")
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("user_registration_date").alias("User_Registration_Date"),
        col("Last_Updated_On")
      )
    generateReportsWithoutPartition(usersWithoutAnyEnrollmentsWithUserDetailsDF, s"${commsConsoleReportPath}/usersWithoutEnrollments", "usersWithoutEnrollments")

    // users created in last 15 days, but not enrolled in any cbp
    val usersCreatedInLastNDaysDF = userDF.filter(col("registrationDate").between(dateNDaysAgo, currentDate)).select("user_id").distinct()
    val usersCreatedInLastNDaysWithoutEnrollmentsDF = usersCreatedInLastNDaysDF.except(usersWithEnrollments)
    val usersCreatedInLastNDaysWithoutEnrollmentsWithUserDetailsDF = usersCreatedInLastNDaysWithoutEnrollmentsDF.join(userDF, Seq("user_id"), "left")
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("user_registration_date").alias("User_Registration_Date"),
        col("Last_Updated_On")
      )
    generateReportsWithoutPartition(usersCreatedInLastNDaysWithoutEnrollmentsWithUserDetailsDF, s"${commsConsoleReportPath}/recentUsersWithoutEnrollments", "recentUsersWithoutEnrollments")

    //top 60 users ranked by cbp completion in last 15 days
    val topXCompletionsInNDays = enrollmentsDF.filter(col("completionDate").between(dateNDaysAgo, currentDate))
      .groupBy("user_id")
      .agg(
        count("*").alias("completionCount")
        )
      .orderBy(desc("completionCount")).limit(numOfTopLearners)
      .join(userDF, Seq("user_id"), "left")
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("user_registration_date").alias("User_Registration_Date"),
        col("completionCount").alias("Content_Completion"),
        col("Last_Updated_On")
      )
    generateReportsWithoutPartition(topXCompletionsInNDays, s"${commsConsoleReportPath}/topNRecentCompletions", "topNRecentCompletions")

    val prarambhCourses = conf.commsConsolePrarambhCbpIds.split(",").map(_.trim).toList
    val rozgarTags =  conf.commsConsolePrarambhTags.split(",").map(_.trim).toList
    val checkForRozgarTag = rozgarTags.map(value => expr(s"lower(tag) like '%$value%'")).reduce(_ or _)
    val checkForKBEmail = expr(s"email LIKE '%$bkEmailSuffix'")
    val rozgarUsersDF = userDF.filter(checkForKBEmail || checkForRozgarTag)
    val prarambhCourseCount = prarambhCourses.size
    val prarambhCompletionCount = conf.commsConsolePrarambhNCount

    val prarambhEnrollments = rawEnrollmentsDF
      .filter(col("user_consumption_status") === "completed" && col("cbp_id").isin(prarambhCourses: _*))
    val prarambEnrollmentsByRozgarUsersDF = rozgarUsersDF.join(prarambhEnrollments, Seq("user_id"), "left")
    val prarambhCompletionCountsDF = prarambEnrollmentsByRozgarUsersDF.groupBy("user_id").agg(
      count("*").alias("prarambhCompletionCount"),
      max("completionDate").alias("Completed_On")
    )
    val prarambhUserDataWithCompletionCountsDF = prarambhCompletionCountsDF.join(rozgarUsersDF, Seq("user_id"), "inner")
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("user_registration_date").alias("User_Registration_Date"),
        col("Completed_On"),
        col("Last_Updated_On"),
        col("prarambhCompletionCount")
      )

    generateReportsWithoutPartition(prarambhUserDataWithCompletionCountsDF.filter(col("prarambhCompletionCount") === prarambhCompletionCount).drop("prarambhCompletionCount")
      , s"${commsConsoleReportPath}/prarambhUsers6Completions", "prarambhUsers6Completions")
    generateReportsWithoutPartition(prarambhUserDataWithCompletionCountsDF.filter(col("prarambhCompletionCount") === prarambhCourseCount).drop("prarambhCompletionCount")
      , s"${commsConsoleReportPath}/prarambhUsersAllCompletions", "prarambhUsersAllCompletions")

    syncReports(s"${conf.localReportDir}/${commsConsoleReportPath}", commsConsoleReportPath)

    Redis.closeRedisConnect()

  }
}
