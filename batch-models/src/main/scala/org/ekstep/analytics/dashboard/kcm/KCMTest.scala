package org.ekstep.analytics.dashboard.kcm

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil
import org.ekstep.analytics.framework.FrameworkContext

object KCMTest extends Serializable{
  def main(args: Array[String]): Unit = {
    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext("KarmaPointsTest", config)
    val res = DashboardUtil.Test.time(test(config));
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    KCMModel.processKCM(System.currentTimeMillis(), config)
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "192.168.3.249:9092",
      "compression" -> "none",
      "topics" -> Map(
        "roleUserCount" -> "dev.dashboards.role.count",
        "orgRoleUserCount" -> "dev.dashboards.org.role.count",
        "allCourses" -> "dev.dashboards.course",
        "userCourseProgramProgress" -> "dev.dashboards.user.course.program.progress",
        "fracCompetency" -> "dev.dashboards.competency.frac",
        "courseCompetency" -> "dev.dashboards.competency.course",
        "expectedCompetency" -> "dev.dashboards.competency.expected",
        "declaredCompetency" -> "dev.dashboards.competency.declared",
        "competencyGap" -> "dev.dashboards.competency.gap",
        "userOrg" -> "dev.dashboards.user.org",
        "org" -> "dev.dashboards.org"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "validation" -> "true",

      "redisHost" -> "192.168.3.249",
      "redisPort" -> "6379",
      "redisDB" -> "12",

      "sparkCassandraConnectionHost" -> "192.168.3.211",
      "sparkDruidRouterHost" -> "192.168.3.91",
      "sparkElasticsearchConnectionHost" -> "192.168.3.211",
      "fracBackendHost" -> "frac-dictionary.karmayogi.nic.in",
      "appPostgresHost" -> "192.168.3.178:5432",
      "appPostgresUsername" -> "sunbird",
      "appPostgresCredential" -> "sunbird",
      "dwPostgresHost" -> "192.168.3.211:5432",
      "dwPostgresSchema" -> "warehouse",

      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "dev_hierarchy_store",

      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",
      "cassandraRatingsTable" -> "ratings",
      "cassandraUserAssessmentTable" -> "user_assessment_data",
      "cassandraKarmaPointsLookupTable" -> "user_karma_points_credit_lookup",
      "cassandraKarmaPointsTable" -> "user_karma_points",
      "cassandraKarmaPointsSummaryTable" -> "user_karma_points_summary",

      "appPostgresSchema" -> "sunbird",
      "postgreCompetencyTable" -> "data_node",
      "postgreCompetencyHierarchyTable" -> "node_mapping",

      "cutoffTime" -> "60.0",

      "sideOutput" -> sideOutput
    )
    modelParams
  }

}
