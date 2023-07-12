package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.ekstep.analytics.dashboard.UserModel.getReportingFrameworkContext
import org.ekstep.analytics.exhaust.UserAssessmentReportJob.getStorageConfig
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.sunbird.cloud.storage.conf.AppConf
import org.ekstep.analytics.framework.Level.INFO
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.DateTimeZone
import org.apache.spark.sql.functions.current_timestamp

object UserJob extends IJob {

  implicit val className: String = "org.ekstep.analytics.dashboard.UserAssessmentJob"
  val jobName = "UserAssessmentJob"

  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
//        implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    //    init()
    val result = UserModel.processData(config)
    println("got result:")
    result.show()
    saveToBlob(result, jobConfig)

    JobLogger.log("Completed User Assessment Job")
  }

  def saveToBlob(reportData: DataFrame, jobConfig: JobConfig): DataFrame = {
    val modelParams = jobConfig.modelParams.get
//    val reportPath: String = modelParams.getOrElse("reportPath", "user-assessment-reports/").asInstanceOf[String]
    val timestamp = current_timestamp()
    val objectKey = AppConf.getConfig("collection.exhaust.store.prefix")
    val storageConfig = getStorageConfig(jobConfig,objectKey)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    reportData.repartition(1).saveToBlobStore(storageConfig, "csv",
      s"report-${timestamp}",
      Option(Map("header" -> "true")), Option(Seq("user_org_id")), None, Option(false) , None, None)
    reportData
  }

  def getDate: String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }
}
