package org.ekstep.analytics.dashboard.report.warehouse

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext


object DataWarehouseModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.warehouse.DataWarehouseModel"
  override def name() = "DataWarehouseModel"

  /**
   * Reading all the reports and saving it to postgres. Overwriting the data in postgres
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val today = getDate()
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"

    var user_details = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.userReportPath}/${today}-warehouse")
    //user_details = user_details.withColumn("user_registration_date", to_date(col("user_registration_date"), "dd/MM/yyyy"))
    user_details = user_details.withColumn("user_registration_date", date_format(col("user_registration_date"), "dd/MM/yyyy HH:mm:ss a"))


    truncateWarehouseTable(conf.dwUserTable)
    saveDataframeToPostgresTable_With_Append(user_details, dwPostgresUrl, conf.dwUserTable, conf.dwPostgresUsername, conf.dwPostgresCredential)


    var content_details = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.courseReportPath}/${today}-warehouse")

    content_details = content_details
      .withColumn("resource_count", col("resource_count").cast("int"))
      .withColumn("total_certificates_issued", col("total_certificates_issued").cast("int"))
      .withColumn("content_rating", col("content_rating").cast("float"))
      //.withColumn("batch_start_date",to_date(col("batch_start_date"), "yyyy-MM-dd"))
      //.withColumn("batch_end_date", to_date(col("batch_end_date"), "yyyy-MM-dd"))
      //.withColumn("last_published_on", to_date(col("last_published_on"), "yyyy-MM-dd"))
      //.withColumn("content_retired_on", to_date(col("content_retired_on"), "yyyy-MM-dd"))
      .withColumn("batch_start_date", date_format(col("batch_start_date"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("batch_end_date", date_format(col("batch_end_date"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("last_published_on", date_format(col("last_published_on"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("content_retired_on", date_format(col("content_retired_on"), "dd/MM/yyyy HH:mm:ss a"))

    content_details = content_details.dropDuplicates(Seq("content_id"))


    truncateWarehouseTable(conf.dwCourseTable)
    saveDataframeToPostgresTable_With_Append(content_details, dwPostgresUrl, conf.dwCourseTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    var enrollment_details = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.userEnrolmentReportPath}/${today}-warehouse")

    enrollment_details = enrollment_details
      .withColumn("content_progress_percentage", col("content_progress_percentage").cast("float"))
      .withColumn("user_rating", col("user_rating").cast("float"))
      .withColumn("resource_count_consumed", col("resource_count_consumed").cast("int"))
      //.withColumn("completed_on", to_date(col("completed_on"), "yyyy-MM-dd"))
      //.withColumn("certificate_generated_on", to_date(col("certificate_generated_on"), "yyyy-MM-dd"))
      //.withColumn("enrolled_on", to_date(col("enrolled_on"), "yyyy-MM-dd"))
      .withColumn("completed_on", date_format(col("completed_on"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("certificate_generated_on", date_format(col("certificate_generated_on"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("enrolled_on", date_format(col("enrolled_on"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("live_cbp_plan_mandate", col("live_cbp_plan_mandate").cast("boolean"))
      .filter(col("content_id").isNotNull)

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
      //.withColumn("completion_date", to_date(col("completion_date"), "dd/MM/yyyy"))
      .withColumn("completion_date", date_format(col("completion_date"), "dd/MM/yyyy HH:mm:ss a"))
      .filter(col("content_id").isNotNull)

    truncateWarehouseTable(conf.dwAssessmentTable)
    saveDataframeToPostgresTable_With_Append(assessment_details, dwPostgresUrl, conf.dwAssessmentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)


    var bp_enrollments = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.blendedReportPath}/${today}-warehouse")

    bp_enrollments = bp_enrollments
      .withColumn("component_progress_percentage", col("component_progress_percentage").cast("float"))
      //.withColumn("offline_session_date", to_date(col("offline_session_date"), "yyyy-MM-dd"))
      //.withColumn("component_completed_on", to_date(col("component_completed_on"), "yyyy-MM-dd"))
      //.withColumn("last_accessed_on", to_date(col("last_accessed_on"), "yyyy-MM-dd"))
      .withColumn("offline_session_date", date_format(col("offline_session_date"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("component_completed_on", date_format(col("component_completed_on"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("last_accessed_on", date_format(col("last_accessed_on"), "dd/MM/yyyy HH:mm:ss a"))
      .withColumnRenamed("instructor(s)_name", "instructors_name")
      .filter(col("content_id").isNotNull)
      .filter(col("user_id").isNotNull)
      .filter(col("batch_id").isNotNull)

    truncateWarehouseTable(conf.dwBPEnrollmentsTable)
    saveDataframeToPostgresTable_With_Append(bp_enrollments, dwPostgresUrl, conf.dwBPEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val cb_plan = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.acbpReportPath}/${today}-warehouse")
    truncateWarehouseTable(conf.dwCBPlanTable)
    saveDataframeToPostgresTable_With_Append(cb_plan, dwPostgresUrl, conf.dwCBPlanTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val orgDwDf = cache.load("orgHierarchy")
      .withColumn("mdo_created_on", to_date(col("mdo_created_on")))
    generateReport(orgDwDf.coalesce(1), s"${conf.orgHierarchyReportPath}/${today}-warehouse")

    truncateWarehouseTable(conf.dwOrgTable)
    saveDataframeToPostgresTable_With_Append(orgDwDf, dwPostgresUrl, conf.dwOrgTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
  }

}
