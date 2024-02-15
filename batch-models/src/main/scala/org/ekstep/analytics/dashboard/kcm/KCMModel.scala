package org.ekstep.analytics.dashboard.kcm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

object KCMModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.kcm.KCMModel"

  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = events.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processKCM(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processKCM(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if(conf.validation == "true") validation = true

    val appPostgresUrl = s"jdbc:postgresql://${conf.appPostgresHost}/${conf.appPostgresSchema}"
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"
    val today = getDate()
    val reportPath = s"${conf.kcmReportPath}/${today}/ContentCompetencyMapping"
    val fileName = "ContentCompetencyMapping"

    // Content - Competency Mapping data
    val categories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program")
    val cbpDetails = allCourseProgramESDataFrame(categories)
      .where("courseStatus IN ('Live', 'Retired')")
      .select(col("courseID"),col("competencyAreaId"),col("competencyThemeId"),col("competencySubThemeId"), col("courseName"))
    // explode area, theme and sub theme seperately
    val areaExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyAreaId) as (pos, competency_area_id)"))
    val themeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyThemeId) as (pos, competency_theme_id)"))
    val subThemeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencySubThemeId) as (pos, competency_sub_theme_id)"))
    // Joining area, theme and subtheme based on position
    val competencyJoinedDF = areaExploded.join(themeExploded, Seq("courseID", "pos")).join(subThemeExploded, Seq("courseID", "pos"))
    // joining with cbpDetails for getting courses with no competencies mapped to it
    val competencyContentMappingDF = cbpDetails
      .join(competencyJoinedDF, Seq("courseID"), "left")
      .dropDuplicates(Seq("courseID","competency_area_id","competency_theme_id","competency_sub_theme_id"))
    val contentMappingDF = competencyContentMappingDF
      .select(col("courseID").alias("course_id"), col("competency_area_id"), col("competency_theme_id"), col("competency_sub_theme_id"))
    show(contentMappingDF, "competency content mapping df")
    truncateWarehouseTable(conf.dwKcmContentTable)
    saveDataframeToPostgresTable_With_Append(contentMappingDF, dwPostgresUrl, conf.dwKcmContentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    // Competency details data with hierarchy
    val jsonSchema = MapType(StringType, StringType)
    // fetch data from data_node(competency details) and node_mapping(hierarchy)
    val competencyDataDF = postgresTableAsDataFrame(appPostgresUrl, conf.postgresCompetencyTable, conf.appPostgresUsername, conf.appPostgresCredential)
      .select(col("id"), col("name"), col("description"), col("additional_properties"))
      .withColumn("jsonThemeType", from_json(col("additional_properties"), jsonSchema)).drop("additional_properties")
    show(competencyDataDF, "competency data df")
    val competencyMappingDF = postgresTableAsDataFrame(appPostgresUrl, conf.postgresCompetencyHierarchyTable, conf.appPostgresUsername, conf.appPostgresCredential)
      .select(col("id"), col("parent_id"), col("child_id"))
    show(competencyMappingDF, "competency mapping DF")

    // making hierarchy
    val competencyHierarchyDF = competencyMappingDF
      .join(competencyMappingDF
        .withColumnRenamed("parent_id", "parent_parent_id")
        .withColumnRenamed("child_id", "parent_child_id"), col("child_id") === col("parent_parent_id"))
      .select(col("parent_id").alias("competency_area_id"), col("child_id").alias("competency_theme_id"), col("parent_child_id").alias("competency_sub_theme_id"))
    show(competencyHierarchyDF, "competency hierarchy DF")

    // enrich hierarchy with details from data_node
    val competencyDetailsDF = competencyHierarchyDF
      .join(competencyDataDF, col("id")===col("competency_area_id"))
      .withColumnRenamed("name","competency_area").withColumnRenamed("description","competency_area_description").drop("id","jsonThemeType")
      .join(competencyDataDF, col("id")===col("competency_theme_id"))
      .withColumnRenamed("name","competency_theme").withColumnRenamed("description","competency_theme_description").drop("id")
      .withColumn("competency_theme_type", col("jsonThemeType.themeType"))
      .join(competencyDataDF, col("id")===col("competency_sub_theme_id"))
      .withColumnRenamed("name","competency_sub_theme").withColumnRenamed("description","competency_sub_theme_description")
      .select("competency_area_id", "competency_area", "competency_area_description", "competency_theme_type",
        "competency_theme_id", "competency_theme", "competency_theme_description", "competency_sub_theme_id", "competency_sub_theme", "competency_sub_theme_description")
    show(competencyDetailsDF, "Competency details dataframe")
    truncateWarehouseTable(conf.dwKcmDictionaryTable)
    saveDataframeToPostgresTable_With_Append(competencyDetailsDF, dwPostgresUrl, conf.dwKcmDictionaryTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    // competency reporting
    val competencyReporting = competencyContentMappingDF.join(competencyDetailsDF, Seq("competency_area_id","competency_theme_id","competency_sub_theme_id"))
      .select(
        col("courseID").alias("content_id"),
        col("courseName").alias("content_name"),
        col("competency_area"),
        col("competency_area_description"),
        col("competency_theme"),
        col("competency_theme_description"),
        col("competency_theme_type"),
        col("competency_sub_theme"),
        col("competency_sub_theme_description")
      ).orderBy("content_id")
    show(competencyReporting, "Competency reporting dataframe")

    generateReportsWithoutPartition(competencyReporting, reportPath, fileName)
    syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)

  }
}
