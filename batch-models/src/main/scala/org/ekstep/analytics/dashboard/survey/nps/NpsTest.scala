package org.ekstep.analytics.dashboard.survey.nps

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil
import org.ekstep.analytics.dashboard.DashboardUtil.getConfigModelParam
import org.ekstep.analytics.framework.FrameworkContext

object NpsTest extends Serializable{

  def main(args: Array[String]): Unit = {

    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext("NpsTest", config)
    val res = DashboardUtil.Test.time(test(config));
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    NpsModel.processUserReport(System.currentTimeMillis(), config)
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "",
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
        "userOrg" -> "dev.dashboards.user.org"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "validation" -> "true",

      "redisHost" -> "10.0.0.6",
      "redisPort" -> "6379",
      "redisDB" -> "12",

      "sparkCassandraConnectionHost" -> "192.168.3.211",
      "sparkDruidRouterHost" -> "192.168.3.91",
      "sparkElasticsearchConnectionHost" -> "192.168.3.211",
      "fracBackendHost" -> "frac-dictionary.karmayogi.nic.in",
      "sparkMongoConnectionHost" -> "192.168.3.178",
      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "dev_hierarchy_store",
      "cassandraUserFeedKeyspace" -> "sunbird_notifications",


      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",
      "cassandraRatingsTable" -> "ratings",
      "cassandraOrgHierarchyTable" -> "org_hierarchy",
      "cassandraUserFeedTable" -> "notification_feed",
      "mongoDatabase" -> "nodebb",
      "mongoDBCollection" -> "objects",

      "key" -> "aws_storage_key",
      "secret" -> "aws_storage_secret",
      "store" -> "s3",
      "container" -> "igot",
      "platformRatingSurveyId" -> "1696404440829",
      

      "mdoIDs" -> "0135071359030722569,01358993635114188855",

      "sideOutput" -> sideOutput
    )
    modelParams
  }
}

