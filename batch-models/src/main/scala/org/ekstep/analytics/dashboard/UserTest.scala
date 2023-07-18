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
      """{"debug":"true","search":{"type":"none"},"model":"org.ekstep.analytics.dashboard.UserAssessmentJob","modelParams":{"store":"s3","storageKeyConfig":"aws_storage_key","reports_storage_key":"aws_storage_secret","reports_storage_secret":"aws_storage_secret","storageSecretConfig":"aws_storage_secret","container":"igot","fileName":"/User-Reports","sparkCassandraConnectionHost":"192.168.3.211","cassandraUserKeyspace":"user","cassandraHierarchyStoreKeyspace":"dev_hierarchy_store","cassandraUserTable":"user", "cassandraUserAssessmentKeyspace":"sunbird","cassandraOrgTable":"organisation","cassandraAssessmentAggregatorKeyspace":"sunbird_courses","cassandraContentHierarchyTable":"content_hierarchy","cassandraUserAssessmentTable":"user_assessment_data","cassandraAssessmentAggregatorTable":"assessment_aggregator","key":"user_assessment/","format":"csv"},"output":[{"to":"file","params":{"file":"user_assessment_report/"}}],"parallelization":8,"appName":"User Assessment Report"}""".stripMargin
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
