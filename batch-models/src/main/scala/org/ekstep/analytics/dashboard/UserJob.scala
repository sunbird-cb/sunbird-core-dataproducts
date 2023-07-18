package org.ekstep.analytics.dashboard
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, StorageConfig}
import org.ekstep.analytics.framework.Level.INFO
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.DateTimeZone
import org.apache.spark.sql.functions.current_timestamp
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.storage.CustomS3StorageService
import org.sunbird.cloud.storage.IStorageService


object UserJob extends IJob {

  implicit val className: String = "org.ekstep.analytics.dashboard.UserJob"
  val jobName = "UserJob"

  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    //    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    //    init()

    val result = UserModel.processData(config)

    result.show()
    //result.repartition(1).write.partitionBy("user_org_id").format("csv").option("header" ,"true").save(s"/home/analytics/reports/assessmentreport-${getDate}")

    saveToBlob(result, jobConfig)

    JobLogger.log("Completed User Assessment Job")
  }

  def saveToBlob(reportData: DataFrame, jobConfig: JobConfig): DataFrame = {
    val modelParams = jobConfig.modelParams.get
    val timestamp = current_timestamp()
    val objectKey = "assessment-report/"
    val storageConfig = getStorageConfig(jobConfig,objectKey)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    println("Uploading reports to blob storage")

    val storageService = StorageUtil.getStorageService(jobConfig)
    // storageService.put("igotlogs", )

    reportData.repartition(1).saveToBlobStore(storageConfig, "csv", s"report-${getDate}",
      Option(Map("header" -> "true")), Option(Seq("user_org_id")),  Option(storageService), Option(false) , None, Option("csv"))
    println("Completed upload to blob ")
    reportData
  }




  def getStorageConfig(config: JobConfig, key: String): StorageConfig = {

    // val storageType = AppConf.getConfig("cloud_storage_type")

    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint_with_protocol")
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val container = "igotlogs"
    val storageKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String];
    val storageSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
    // val store = modelParams.getOrElse("store", "local").asInstanceOf[String]
    val store = "s3"
    StorageConfig(store, container, key, Option(storageKey), Option(storageSecret))
  }

  def getDate: String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }
}
