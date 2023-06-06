package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import DashboardUtil._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.ekstep.analytics.framework.{AlgoInput, AlgoOutput, FrameworkContext, IBatchModelTemplate, Output}

import java.io.Serializable

case class CCDummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class CCDummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

case class CCConfig( debug: String,
                     sparkElasticsearchConnectionHost: String, cassandraKeyspace: String, cassandraTable: String, sparkDruidRouterHost: String
                   ) extends Serializable

object CuratedCollectionsModel extends IBatchModelTemplate[String, CCDummyInput, CCDummyOutput, CCDummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.CuratedCollectionsModel"

  override def name() = "CuratedCollectionsModel"

  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CCDummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(CCDummyInput(executionTime)))
  }

  override def algorithm(data: RDD[CCDummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CCDummyOutput] = {
    val timestamp = data.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCuratedCollectionsData(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(events: RDD[CCDummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CCDummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  def processCuratedCollectionsData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: CCConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config

    val curatedCollectionDataDf = curatedCollectionReportDf()
    curatedCollectionDataDf.repartition(1).write.option("header", true).csv("tmp/output/CuratedCollectionReport")
  }

  def curatedCollectionEsDf()(implicit spark: SparkSession, conf: CCConfig): DataFrame = {
    val query =
      """{"_source":["identifier","name","primaryCategory"],
        |"query":{"bool":{"must":[{"match":{"primaryCategory.raw":"CuratedCollections"}}, {"exists": {"field": "contentTypesCount"}}]}}}""".stripMargin

    val fields = Seq("name", "primaryCategory", "identifier")
    var df = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields)
    show(df, "Curated collection IDs from Elasticsearch")
    df
  }

  val hierarchySchema = new StructType()
    .add("name", StringType)
    .add("duration", StringType)
    .add("children", ArrayType(new StructType()
      .add("identifier", StringType)
      .add("name", StringType)
    ))

  def curatedCollectionsDbDf()(implicit spark: SparkSession, config: CCConfig): DataFrame = {
    var df1 = curatedCollectionEsDf().distinct()
    var df = cassandraTableAsDataFrame("dev_hierarchy_store", "content_hierarchy").select(col("identifier"), col("hierarchy"))
    df = df1.join(df, Seq("identifier"), "inner")
    df = df.filter(col("hierarchy").isNotNull)

    df = df.withColumn("jsonData", from_json(col("hierarchy"), hierarchySchema))
    df = df.select("jsonData.*", "identifier")

    df = df.withColumn("courseID", explode(df.col("children.identifier")))
    df = df.select(col("identifier"), col("courseID"), col("name").alias("Collection Name"), col("duration"))
    show(df, "Curated Collections Course Data")
    df
  }


  def courseDataDf()(implicit spark: SparkSession, config: CCConfig): DataFrame = {
    val query =
      """SELECT __time,courseID,courseName,dbCompletionStatus FROM \"dashboards-user-course-program-progress\"""".stripMargin
    var df = druidDFOption(query, config.sparkDruidRouterHost).orNull
    df = df.join(curatedCollectionsDbDf(), Seq("courseID"), "inner")
    show(df, "after join with cassandra data")
    df
  }

  def curatedCollectionReportDf()(implicit spark: SparkSession, config: CCConfig): DataFrame = {
    var df = courseDataDf()
    df = df.join(totalEnrollments(df), Seq("identifier"), "inner")
    df = df.join(totalCompletions(df), Seq("identifier"), "inner")

    df = df.drop("courseID", "dbCompletionStatus", "identifier", "courseName", "__time")
    df = df.dropDuplicates()
    df = df.withColumn("Completion to Enrollment Ratio", round(
      col("Number of Officers Completed") / col("Number of Officers Enrolled (including Completed)"), 2))

    show(df, "after adding enrollment and content count and completion count to data")
    df
  }

  def totalCompletions(data: DataFrame)(implicit spark: SparkSession, config: CCConfig): DataFrame = {
    var df = data.groupBy(col("identifier")).pivot(col("dbCompletionStatus"), Seq("2"))
      .count()
    df = df.withColumnRenamed("2", "Number of Officers Completed")
    show(df, "Total completion")
    df
  }

  def totalEnrollments(data: DataFrame)(implicit spark: SparkSession, config: CCConfig): DataFrame = {
    var df = data.groupBy(col("identifier"))
      .agg(count("dbCompletionStatus").alias("Number of Officers Enrolled (including Completed)"),
        countDistinct("courseID").alias("Number of course contents"))
    show(df, "collection Enrollments")
    df
  }


  def parseConfig(config: Map[String, AnyRef]): CCConfig = {
    CCConfig(
      debug = getConfigModelParam(config, "debug"),
      sparkElasticsearchConnectionHost = getConfigModelParam(config, "sparkElasticsearchConnectionHost"),
      cassandraKeyspace = getConfigModelParam(config, "cassandraKeySpace"),
      cassandraTable = getConfigModelParam(config, "cassandraTable"),
      sparkDruidRouterHost = getConfigModelParam(config, "sparkDruidRouterHost")
    )
  }
}
