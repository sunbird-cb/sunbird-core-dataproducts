package org.ekstep.analytics.dashboard.kcm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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

    // Content - Competency Mapping data
    val categories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program")
    val cbpDetails = allCourseProgramESDataFrame(categories)
      .where("courseStatus IN ('Live', 'Retired')")
      .select(col("courseID"),col("competencyAreaId"),col("competencyThemeId"),col("competencySubThemeId"))
    // explode area, theme and sub theme seperately
    val areaExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyAreaId) as (pos, competency_area_id)"))
    val themeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyThemeId) as (pos, competency_theme_id)"))
    val subThemeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencySubThemeId) as (pos, competency_sub_theme_id)"))
    // Joining area, theme and subtheme based on position
    val competencyJoinedDF = areaExploded.join(themeExploded, Seq("courseID", "pos")).join(subThemeExploded, Seq("courseID", "pos"))
    // joining with cbpDetails for getting courses with no competencies mapped to it
    val competencyContentMappingDF = cbpDetails
      .join(competencyJoinedDF, Seq("courseID"), "left")
      .select(col("courseID").alias("course_id"), col("competency_area_id"), col("competency_theme_id"), col("competency_sub_theme_id"))
      .dropDuplicates(Seq("course_id","competency_area_id","competency_theme_id","competency_sub_theme_id"))

//    saveDataframeToPostgresTable_With_Append(competencyContentMappingDF, dwPostgresUrl, conf.dwOrgTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    // Competency details data with hierarchy
    // fetch data from data_node(competency details) and node_mapping(hierarchy)
    val competencyDataDF = postgresTableAsDataFrame(appPostgresUrl, conf.postgreCompetencyTable, conf.appPostgresUsername, conf.appPostgresCredential)
      .select(col("id"), col("type"), col("name"), col("description"))
    val competencyMappingDF = postgresTableAsDataFrame(appPostgresUrl, conf.postgreCompetencyHierarchyTable, conf.appPostgresUsername, conf.appPostgresCredential)
      .select(col("id"), col("parent_id"), col("child_id"))

    // making hierarchy
    val competencyHierarchyDF = competencyMappingDF
      .join(competencyMappingDF
        .withColumnRenamed("parent_id", "parent_parent_id")
        .withColumnRenamed("child_id", "parent_child_id"), col("child_id") === col("parent_parent_id"))
      .select(col("parent_id").alias("competency_area_id"), col("child_id").alias("competency_theme_id"), col("parent_child_id").alias("competency_sub_theme_id"))

    // enriching hierarchy with details from data_node
    val competencyDetailsDF = competencyDataDF
      .join(competencyHierarchyDF, col("id")===col("competency_area_id"))
      .withColumnRenamed("name","competency_area").withColumnRenamed("description","competency_area_description").withColumnRenamed("type","competency_type")
      .join(competencyHierarchyDF, col("id")===col("competency_theme_id"))
      .withColumnRenamed("name","competency_theme").withColumnRenamed("description","competency_theme_description").withColumnRenamed("type","competency_theme_type")
      .join(competencyHierarchyDF, col("id")===col("competency_sub_theme_id"))
      .withColumnRenamed("name","competency_sub_theme").withColumnRenamed("description","competency_sub_theme_description").withColumnRenamed("type","competency_sub_theme_type")
      .select("competency_area_id", "competency_area", "competency_area_description", "competency_type",
        "competency_theme_id", "competency_theme", "competency_theme_description", "competency_sub_theme_id", "competency_sub_theme", "competency_sub_theme_description")

//    saveDataframeToPostgresTable_With_Append(competencyDetailsDF, dwPostgresUrl, conf.dwOrgTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

  }
}
