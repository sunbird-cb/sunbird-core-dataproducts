package org.ekstep.analytics.dashboard

import org.apache.spark.sql.functions.{col, explode, expr, from_json, lit}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.ekstep.analytics.dashboard.DashboardUtil.{cassandraTableAsDataFrame, show}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.{JobConfig, JobContext}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.exhaust.BaseReportsJob

object UserModel extends BaseReportsJob {

  def processData(config: String)(implicit fc: Option[FrameworkContext]): DataFrame = {
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    assessmentReportDf()
  }

  def assessmentReportDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    var df = userAssessmentDataDf().join(courseDataDbDf(), Seq("course_id"))
      .join(userDataDbDf(), Seq("user_id"))
//      .join(contentDataDbDf(), Seq("content_id"))
//    df = df.join(assessmentStatusDf(), Seq("user_id"))
    df = df.distinct()
    show(df, "Assessment Report")
    df
  }

  /**
   * Fetch data from cassandra assessment table
   *
   * @param spark
   * @param conf
   * @return Dataframe with assessment related data from Cassandra - course_id, user_id, content_id, attempt_id, total_max_score, total_score
   */
//  def assessmentDbDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
//    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
//    var df = cassandraTableAsDataFrame(modelParams.get("cassandraAssessmentAggregatorKeyspace").toString,
//      modelParams.get("cassandraAssessmentAggregatorTable").toString)
//      .select(col("course_id"), col("user_id"), col("content_id"), col("attempt_id"),
//        col("total_max_score"), col("total_score"))
//    df = df.withColumn("percentage_score", 100 * (col("total_max_score").divide(col("total_score"))))
//
//    show(df, "Assessment data from cassandra")
//    df
//  }


  val submitassessmentrequestSchema: StructType = StructType(Seq(
    StructField("primaryCategory", StringType, nullable = false),
    StructField("courseId", StringType, nullable = false)
))

  val submitassessmentresponseSchema: StructType = StructType(Seq(
    StructField("pass", BooleanType, nullable = false),
    StructField("result", DoubleType, nullable = false)  // check the datatype
  ))

  def userAssessmentDataDf()(implicit spark:SparkSession, config: JobConfig): DataFrame = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    var df = cassandraTableAsDataFrame(modelParams.get("cassandraUserAssessmentKeyspace").toString,
      modelParams.get("cassandraUserAssessmentTable").toString)
      .select(col("userid").alias("user_id"), col("assessmentid"), col("status").alias("assessment_status"),
        col("submitassessmentresponse"), col("submitassessmentrequest"))
    df = df.withColumn("requestData", from_json(col("submitassessmentrequest"), submitassessmentrequestSchema))
    df = df.select(
      col("requestData.primaryCategory").alias("type"),
      col("requestData.courseId").alias("course_id")
    )

    df = df.withColumn("responseData", from_json(col("submitassessmentresponse"), submitassessmentresponseSchema))
    df = df.select(
      col("responseData.pass").alias("pass"),
      col("responseData.result").alias("percentage_score")
    )

    val caseExpression = "CASE WHEN pass == true THEN pass == 'Pass' ELSE pass == 'Fail' END"
    df = df.withColumn("assessment_status", expr(caseExpression))

    df = df.select(
      col("user_id"), col("assessmentid"), col("assessment_status"), col("type"), col("course_id"),
      col("percentage_score"), col("assessment_status")
    )
    df
 }

  /**
   * Schema for course data
   */
  val courseHierarchySchema: StructType = StructType(Seq(
    StructField("identifier", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("channel", StringType, nullable = true)
  ))

  /**
   * Fetch course-content data from cassandra hierarchy table
   *
   * @param spark
   * @param config
   * @return Dataframe - course_id, hierarchy, user_id, content_id, attempt_id, total_max_score, total_score
   */
  def hierarchyDataDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    var df = cassandraTableAsDataFrame(modelParams.get("cassandraHierarchyStoreKeyspace").toString, modelParams.get("cassandraContentHierarchyTable").toString)
      .select(col("identifier").alias("course_id"), col("hierarchy"))
    df = df.filter(col("hierarchy").isNotNull)

    df = df.join(userAssessmentDataDf(), Seq("course_id"), "inner")
    show(df, "Course - content data from Cassandra")
    df
  }

  /**
   * Fetch course data from cassandra hierarchy table and join with organisation related data
   *
   * @param spark
   * @param config
   * @return Dataframe - course_id, course_name, course_org_id, course_org_name, course_org_status
   */
  def courseDataDbDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    var df = hierarchyDataDf()
    df = df.withColumn("data", from_json(col("hierarchy"), courseHierarchySchema))
    df = df.select(
      col("course_id"),
      col("data.name").alias("course_name"),
      col("data.channel").alias("rootorgid"),
      col("data.primaryCategory").alias("course_category")
    )
    df = df.join(organisationDataDbDf(), Seq("rootorgid"), "inner")
    df = df.select(
      col("course_id"), col("course_name"), col("rootorgid").alias("course_org_id"),
      col("org_name").alias("course_org_name"),
//      col("org_status").alias("course_org_status"),
      col("course_category")
    )
    show(df, "Course related data from Cassandra")
    df
  }

  /**
   * Schema for content data
   */
//  val contentHierarchySchema = new StructType()
//    .add("identifier", StringType)
//    .add("children", ArrayType(new StructType()
//      .add("identifier", StringType)
//      .add("name", StringType)
//    ))

  /**
   * Fetch content related data
   *
   * @param spark
   * @param config
   * @return Dataframe - content_id, content_name
   */
//  def contentDataDbDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
//    var df = hierarchyDataDf()
//    df = df.withColumn("content", from_json(col("hierarchy"), contentHierarchySchema))
//
//    df = df.select("content.*", "course_id")
//    df = df.select(explode(df.col("children")))
//    df = df.select(df("col.identifier").alias("content_id"), df("col.name").alias("content_name"))
//    df = df.distinct()
//    show(df, "Content related data from cassandra")
//    df
//  }

  /**
   * Fetch user related data from Cassandra User table and joining with organisation related dataframe
   *
   * @param spark
   * @param config
   * @return Dataframe - user_id, firstname, lastname, user_status, user_org_id, user_org_name, user_org_status
   */
  def userDataDbDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    var df = cassandraTableAsDataFrame(modelParams.get("cassandraUserAssessmentKeyspace").toString, modelParams.get("cassandraUserTable").toString)
      .select(col("userid").alias("user_id"), col("firstname"), col("lastname"),
        col("status"), col("rootorgid"), col("maskedemail"), col("maskedphone"))
    df = df.join(organisationDataDbDf(), Seq("rootorgid"), "inner")
//    val caseExpression = "CASE WHEN status == 0 THEN 'Inactive' WHEN status == 1 THEN 'Active' ELSE 'Status unknown' END"
//    df = df.withColumn("user_status", expr(caseExpression))
    df = df.withColumn("full_name", functions.concat(col("firstname"), lit(' '), col("lastname")))
    df = df.drop("status", "firstname", "lastname")
    df = df.select(
      col("user_id"), col("full_name"),
//      col("user_status"),
      col("rootorgid").alias("user_org_id"), col("org_name").alias("user_org_name"),
//      col("org_status").alias("user_org_status"),
      col("maskedphone").alias("mobile_no"),
      col("maskedemail").alias("email")
    )
    show(df, "User related data from Cassandra")
    df
  }

  /**
   * Fetch organisation related data from cassandra organisation table
   *
   * @param spark
   * @param config
   * @return Dataframe - rootorgid, org_name, org_status
   */
  def organisationDataDbDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    var df = cassandraTableAsDataFrame(modelParams.get("cassandraUserAssessmentKeyspace").toString, modelParams.get("cassandraOrgTable").toString)
      .select(col("rootorgid"), col("orgname").alias("org_name"), col("status"))
//    val caseExpression = "CASE WHEN status == 0 THEN 'Inactive' WHEN status == 1 THEN 'Active' ELSE 'Status unknown' END"
//    df = df.withColumn("org_status", expr(caseExpression))
//    df = df.drop("status")
    df
  }

}
