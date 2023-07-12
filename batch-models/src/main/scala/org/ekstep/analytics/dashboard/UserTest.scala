package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}

object UserTest {

  def main(args: Array[String]): Unit = {
        val cassandraHost = "localhost"
    val jobConf: JobConfig = JSONUtils.deserialize[JobConfig](testModelConfig())
    implicit val spark =
      SparkSession
        .builder()
        .appName("UserTest")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host", cassandraHost)
        .config("spark.cassandra.output.batch.size.rows", "10000")
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val fc: FrameworkContext = new FrameworkContext()
    sc.setLogLevel("WARN")
    val res = time(test());
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def testModelConfig() = {
    val modelParams =
//      """{"search":{"type":"none"},"model":"org.ekstep.analytics.dashboard.UserJob","modelParams":{"debug":"false","validation":"false","storageKeyConfig":"{{sunbird_private_s3_storage_key}}","storageSecretConfig":"{{sunbird_private_s3_storage_secret}}","redisHost":"10.0.0.6","redisPort":"6379","redisDB":"12","sparkCassandraConnectionHost":"10.0.0.7","cassandraUserKeyspace":"user","cassandraHierarchyStoreKeyspace":"dev_hierarchy_store","cassandraUserTable":"user","cassandraUserAssessmentKeyspace":"sunbird","cassandraOrgTable":"organisation","cassandraAssessmentAggregatorKeyspace":"sunbird_courses", "store":"ceph", "cassandraContentHierarchyTable":"content_hierarchy", "cassandraUserAssessmentTable":"user_assessment_data","cassandraAssessmentAggregatorTable":"assessment_aggregator","sideOutput":{"brokerList":"'$brokerList'","compression":"{{ dashboards_broker_compression }}","output":[],"parallelization":10,"appName":"UserJob","deviceMapping":false}""".stripMargin
//      """{"search":{"type":"none"},"model":"org.ekstep.analytics.dashboard.UserAssessmentJob","modelParams":{"store":"ceph","fileName": "","storageKeyConfig":"{{sunbird_private_s3_storage_key}}","storageSecretConfig":"{{sunbird_private_s3_storage_secret}}","container":"test-container","sparkCassandraConnectionHost":"10.0.0.7","cassandraUserKeyspace":"user","cassandraHierarchyStoreKeyspace":"dev_hierarchy_store","cassandraUserTable":"user","cassandraUserAssessmentKeyspace":"sunbird","cassandraOrgTable":"organisation","cassandraAssessmentAggregatorKeyspace":"sunbird_courses", "cassandraContentHierarchyTable":"content_hierarchy", "cassandraUserAssessmentTable":"user_assessment_data","cassandraAssessmentAggregatorTable":"assessment_aggregator","key":"user_assessment/","format":"csv"},"output":[{"to":"file","params":{"file":"user_assessment_report/"}}],"parallelization":8,"appName":"User Assessment Report"}""".stripMargin
   """{"search":{"type":"none"},"model":"org.ekstep.analytics.dashboard.UserAssessmentJob","modelParams":{"store":"ceph","storageKeyConfig":"{{sunbird_private_s3_storage_key}}","storageSecretConfig":"{{sunbird_private_s3_storage_secret}}","container":"{{s3_storage_container}}","fileName":"/user_assessment_reports","sparkCassandraConnectionHost":"10.0.0.7","cassandraUserKeyspace":"user","cassandraHierarchyStoreKeyspace":"dev_hierarchy_store","cassandraUserTable":"user","cassandraUserAssessmentKeyspace":"sunbird","cassandraUserAssessmentTable":"user_assessment_data","cassandraOrgTable":"organisation","cassandraAssessmentAggregatorKeyspace":"sunbird_courses","cassandraContentHierarchyTable":"content_hierarchy","cassandraAssessmentAggregatorTable":"assessment_aggregator","key":"user_assessment/","format":"csv"},"output":[{"to":"file","params":{"file":"user_assessment_report/"}}],"parallelization":8,"appName":"User Assessment Report"}""".stripMargin
    modelParams
  }

  def test()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    val timestamp = System.currentTimeMillis()
    val config = testModelConfig()
    UserJob.main(config)
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }
}
