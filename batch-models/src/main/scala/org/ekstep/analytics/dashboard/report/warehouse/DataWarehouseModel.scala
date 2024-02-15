package org.ekstep.analytics.dashboard.report.warehouse

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object DataWarehouseModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.warehouse.DataWarehouseModel"
  override def name() = "DataWarehouseModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()


    syncReportsToPostgres(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /**
   * Reading all the reports and saving it to postgres. Overwriting the data in postgres
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def syncReportsToPostgres(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    implicit val conf: DashboardConfig = parseConfig(config)
    val today = getDate()
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"
    val appPostgresUrl = s"jdbc:postgresql://${conf.appPostgresHost}/${conf.appPostgresSchema}"

    var user_details = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.userReportPath}/${today}-warehouse")
    user_details = user_details.withColumn("user_registration_date", to_date(col("user_registration_date"), "dd/MM/yyyy"))

    truncateWarehouseTable(conf.dwUserTable)
    saveDataframeToPostgresTable_With_Append(user_details, dwPostgresUrl, conf.dwUserTable, conf.dwPostgresUsername, conf.dwPostgresCredential)


    var cbp_details = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.courseReportPath}/${today}-warehouse")

    cbp_details = cbp_details
      .withColumn("resource_count", col("resource_count").cast("int"))
      .withColumn("total_certificates_issued", col("total_certificates_issued").cast("int"))
      .withColumn("cbp_rating", col("cbp_rating").cast("float"))
      .withColumn("batch_start_date",to_date(col("batch_start_date"), "yyyy-MM-dd"))
      .withColumn("batch_end_date", to_date(col("batch_end_date"), "yyyy-MM-dd"))
      .withColumn("last_published_on", to_date(col("last_published_on"), "yyyy-MM-dd"))
      .withColumn("cbp_retired_on", to_date(col("cbp_retired_on"), "yyyy-MM-dd"))

    cbp_details = cbp_details.dropDuplicates(Seq("cbp_id"))


    truncateWarehouseTable(conf.dwCourseTable)
    saveDataframeToPostgresTable_With_Append(cbp_details, dwPostgresUrl, conf.dwCourseTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    var enrollment_details = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.userEnrolmentReportPath}/${today}-warehouse")

    enrollment_details = enrollment_details
      .withColumn("cbp_progress_percentage", col("cbp_progress_percentage").cast("float"))
      .withColumn("user_rating", col("user_rating").cast("float"))
      .withColumn("resource_count_consumed", col("resource_count_consumed").cast("int"))
      .withColumn("completed_on", to_date(col("completed_on"), "yyyy-MM-dd"))
      .withColumn("certificate_generated_on", to_date(col("certificate_generated_on"), "yyyy-MM-dd"))
      .withColumn("enrolled_on", to_date(col("enrolled_on"), "yyyy-MM-dd"))
      .filter(col("cbp_id").isNotNull)

    truncateWarehouseTable(conf.dwEnrollmentsTable)
    saveDataframeToPostgresTable_With_Append(enrollment_details, dwPostgresUrl, conf.dwEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    var assessment_details = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.cbaReportPath}/${today}-warehouse")

    assessment_details = assessment_details
      .withColumn("score_achieved", col("score_achieved").cast("float"))
      .withColumn("overall_score", col("overall_score").cast("float"))
      .withColumn("cut_off_percentage", col("cut_off_percentage").cast("float"))
      .withColumn("total_question", col("total_question").cast("int"))
      .withColumn("number_of_incorrect_responses", col("number_of_incorrect_responses").cast("int"))
      .withColumn("number_of_retakes", col("number_of_retakes").cast("int"))
      .withColumn("completion_date", to_date(col("completion_date"), "dd/MM/yyyy"))
      .filter(col("cbp_id").isNotNull)

    truncateWarehouseTable(conf.dwAssessmentTable)
    saveDataframeToPostgresTable_With_Append(assessment_details, dwPostgresUrl, conf.dwAssessmentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)


    var bp_enrollments = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.blendedReportPath}/${today}-warehouse")

    bp_enrollments = bp_enrollments
      .withColumn("component_progress_percentage", col("component_progress_percentage").cast("float"))
      .withColumn("offline_session_date", to_date(col("offline_session_date"), "yyyy-MM-dd"))
      .withColumn("component_completed_on", to_date(col("component_completed_on"), "yyyy-MM-dd"))
      .withColumn("last_accessed_on", to_date(col("last_accessed_on"), "yyyy-MM-dd"))
      .withColumnRenamed("instructor(s)_name", "instructors_name")
      .filter(col("cbp_id").isNotNull)
      .filter(col("user_id").isNotNull)
      .filter(col("batch_id").isNotNull)

    truncateWarehouseTable(conf.dwBPEnrollmentsTable)
    saveDataframeToPostgresTable_With_Append(bp_enrollments, dwPostgresUrl, conf.dwBPEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val orgDf = postgresTableAsDataFrame(appPostgresUrl, conf.appOrgHierarchyTable, conf.appPostgresUsername, conf.appPostgresCredential)

    val orgCassandraDF = orgDataFrame().select(col("orgID").alias("sborgid"), col("orgType"), col("orgName").alias("cassOrgName"), col("orgCreatedDate"))
    val orgDfWithOrgType = orgCassandraDF.join(orgDf, Seq("sborgid"), "left")

    var orgDwDf = orgDfWithOrgType.select(
        col("sborgid").alias("mdo_id"),
        col("cassOrgName").alias("mdo_name"),
        col("l1orgname").alias("ministry"),
        col("l2orgname").alias("department"),
        to_date(to_timestamp(col("orgCreatedDate"))).alias("mdo_created_on"),
        col("orgType")
      )
      .withColumn("is_cbp_provider",
        when(col("orgType").cast("int") === 128 || col("orgType").cast("int") === 128, lit("Y")).otherwise(lit("N")))
      .withColumn("organization", when(col("ministry").isNotNull && col("department").isNotNull, col("mdo_name")).otherwise(null))
      .withColumn("data_last_generated_on", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss a"))
      .distinct()

    orgDwDf = orgDwDf.drop("orgType")

    orgDwDf = orgDwDf.dropDuplicates(Seq("mdo_id"))

    val orgReportPath = s"${conf.orgHierarchyReportPath}/${today}"
    generateWarehouseReport(orgDwDf.coalesce(1), orgReportPath, conf.localReportDir)
    truncateWarehouseTable(conf.dwOrgTable)

    saveDataframeToPostgresTable_With_Append(orgDwDf, dwPostgresUrl, conf.dwOrgTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
  }

}
