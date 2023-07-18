package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode, expr, from_json}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import DashboardUtil._
import org.ekstep.analytics.framework.{AlgoInput, AlgoOutput, FrameworkContext, IBatchModelTemplate, Output}

import java.io.Serializable

case class CCDummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class CCDummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

case class CCConfig(debug: String, broker: String, compression: String,
                     redisHost: String, redisPort: Int, redisDB: Int,
                     sparkElasticsearchConnectionHost: String, sparkCassandraConnectionHost: String,
                     cassandraHierarchyStoreKeyspace: String, cassandraContentHierarchyTable: String, sparkDruidRouterHost: String,
                     curatedCollectionKafkaTopic: String, cassandraOrgKeyspace: String, cassandraOrgTable: String
                   ) extends DashboardConfig

/**
 * Curated collections dashboard Model
 */
object CuratedCollectionsDashboardModel extends IBatchModelTemplate[String, CCDummyInput, CCDummyOutput, CCDummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.CuratedCollectionsDashboardModel"

  override def name() = "CuratedCollectionsDashboardModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CCDummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(CCDummyInput(executionTime)))
  }

  override def algorithm(data: RDD[CCDummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CCDummyOutput] = {
    val timestamp = data.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCuratedCollectionsDashboardData(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(data: RDD[CCDummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CCDummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /**
   * Master method for curated collections data
   * @param timestamp unique timestamp
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processCuratedCollectionsDashboardData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: CCConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config

    // Get curated collections dataframe and dispatch to kafka to be ingested by Druid - dashboards-curated-collections
    val curatedCollectionsDataDF = curatedCollectionsCourseDataDf()
    kafkaDispatch(withTimestamp(curatedCollectionsDataDF, timestamp), conf.curatedCollectionKafkaTopic)
  }

  /**
   * Get collection data from Elasticsearch
   * @return Dataframe from Elasticsearch with columns - collectionID, collectionName, collectionOrgID, collectionOrgName,
   *         collectionCategory, collectionStatus, collectionDuration
   */
  def curatedCollectionEsDf()(implicit spark: SparkSession, conf: CCConfig): DataFrame = {
    val query =
      """{"_source":["identifier","name","primaryCategory","status","channel","duration"],
        |"query":{"bool":{"must":[{"match":{"primaryCategory.raw":"CuratedCollections"}}, {"exists": {"field": "contentTypesCount"}}]}}}""".stripMargin

    val fields = Seq("name", "primaryCategory", "identifier", "status", "channel", "duration")
    var df = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields)

    df = df.select(col("identifier").alias("collection_id"),col("name").alias("collection_name"),
      col("channel").alias("rootorgid"), col("primaryCategory").alias("collection_category"),
      col("status").alias("collection_status"), col("duration").alias("collection_duration"))

    df = df.join(organisationDataDbDf(), Seq("rootorgid"), "inner")
    df = df.select(col("collection_id"), col("collection_name"),
      col("rootorgid").alias("collection_org_id"), col("orgname").alias("collection_org_name"),
      col("org_status").alias("collection_org_status"), col("collection_category"),
      col("collection_status"), col("collection_duration"))

    show(df, "Curated Collections data from Elasticsearch")
    df
  }

  /**
   * Schema for collection - course Ids
   */
  val hierarchySchema = new StructType()
    .add("name", StringType)
    .add("children", ArrayType(new StructType()
      .add("identifier", StringType)
      .add("name", StringType)
    ))

  /**
   * Schema for course data
   */
  val courseHierarchySchema: StructType = StructType(Seq(
    StructField("identifier", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("duration", StringType, nullable = true),
    StructField("leafNodesCount", IntegerType, nullable = true),
    StructField("primaryCategory", StringType, nullable = true),
    StructField("reviewStatus", StringType, nullable = true)
  ))

  /**
   * Extract all courseIDs related to collection ids from cassandra table dev_hierarchy_store/content_hierarchy
   * @return dataframe with columns - collection_id, course_id
   */
  def curatedCollectionsDbDf()(implicit spark: SparkSession, config: CCConfig): DataFrame = {
    var df1 = curatedCollectionEsDf().distinct()
    var df = cassandraTableAsDataFrame("dev_hierarchy_store", "content_hierarchy")
      .select(col("identifier").alias("collection_id"), col("hierarchy"))
    df = df1.join(df, Seq("collection_id"), "inner")
    df = df.filter(col("hierarchy").isNotNull)
    df = df.withColumn("jsonData", from_json(col("hierarchy"), hierarchySchema))
    df = df.select("jsonData.*", "collection_id")

    df = df.withColumn("course_id", explode(df.col("children.identifier")))
    df = df.select(col("collection_id"), col("course_id"))
    show(df, "Curated Collections Course IDs")
    df
  }

  /**
   * Get course details from cassandra table dev_hierarchy_store/content_hierarchy
   * @return course dataframe with columns - course_id, course_name, course_status, course_duration, course_resource_count,
   *         course_org_id, course_org_name, course_org_status, course_category, course_review_status
   */
  def courseDbDf()(implicit spark:SparkSession, config: CCConfig): DataFrame = {
    var df = cassandraTableAsDataFrame("dev_hierarchy_store", "content_hierarchy")
      .select(col("identifier").alias("course_id"), col("hierarchy"))
    df = curatedCollectionsDbDf().join(df, Seq("course_id"), "inner")
    df = df.filter(col(
"hierarchy").isNotNull)
    df = df.withColumn("data", from_json(col("hierarchy"), courseHierarchySchema))
    df = df.select(
      col("course_id"),
      col("data.name").alias("course_name"),
      col("data.status").alias("course_status"),
      col("data.duration").cast(FloatType).alias("course_duration"),
      col("data.leafNodesCount").alias("course_resource_count"),
      col("data.channel").alias("rootorgid"),
      col("data.primaryCategory").alias("course_category"),
      col("data.reviewStatus").alias("course_review_status")
    )
    df = df.join(organisationDataDbDf(), Seq("rootorgid"), "inner")
    df = df.select(
      col("course_id"),
      col("course_name"),
      col("course_status"),
      col("course_duration"),
      col("course_resource_count"),
      col("rootorgid").alias("course_org_id"),
      col("orgname").alias("course_org_name"),
      col("org_status").alias("course_org_status"),
      col("course_category"),
      col("course_review_status")
    )
    show(df, "Course data")
    df
  }

  def organisationDataDbDf()(implicit spark: SparkSession, config: CCConfig): DataFrame = {
    var df = cassandraTableAsDataFrame(config.cassandraOrgKeyspace, config.cassandraOrgTable)
      .select(col("rootorgid"), col("orgname"),
        col("status").alias("org_status"))
    df
  }

  /**
   * @return Curated collections dataframe combining collection and course data
   */
  def curatedCollectionsCourseDataDf()(implicit spark: SparkSession, config: CCConfig): DataFrame = {
    var df = courseDbDf().distinct()
    df = df.join(curatedCollectionsDbDf(), Seq("course_id"))
    df = df.join(curatedCollectionEsDf(), "collection_id")
    show(df, "Curated collection with course details")
    df
  }

  def parseConfig(config: Map[String, AnyRef]): CCConfig = {
    CCConfig(
      debug = getConfigModelParam(config, "debug"),
      broker = getConfigSideBroker(config),
      compression = getConfigSideBrokerCompression(config),
      redisHost = getConfigModelParam(config, "redisHost"),
      redisPort = getConfigModelParam(config, "redisPort").toInt,
      redisDB = getConfigModelParam(config, "redisDB").toInt,
      sparkElasticsearchConnectionHost = getConfigModelParam(config, "sparkElasticsearchConnectionHost"),
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      cassandraHierarchyStoreKeyspace = getConfigModelParam(config, "cassandraHierarchyStoreKeyspace"),
      cassandraContentHierarchyTable = getConfigModelParam(config, "cassandraContentHierarchyTable"),
      sparkDruidRouterHost = getConfigModelParam(config, "sparkDruidRouterHost"),
      curatedCollectionKafkaTopic = getConfigModelParam(config, "curatedCollections"),
      cassandraOrgKeyspace = getConfigModelParam(config, "cassandraOrgKeyspace"),
      cassandraOrgTable = getConfigModelParam(config, "cassandraOrgTable")
    )
  }
}
