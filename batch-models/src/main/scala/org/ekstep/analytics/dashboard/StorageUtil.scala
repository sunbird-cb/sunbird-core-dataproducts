package org.ekstep.analytics.dashboard

import org.ekstep.analytics.dashboard.DashboardConfig
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.storage.CustomS3StorageService
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path}

object StorageUtil {

  def getStorageService(config: DashboardConfig): BaseStorageService = {
    //    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint_with_protocol")
    val storageType = "s3"
    //    val storageKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String]
    //    val storageSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
    val storageKey = config.key
    val storageSecret = config.secret

    val storageService = if ("s3".equalsIgnoreCase(storageType) && !"".equalsIgnoreCase(storageEndpoint)) {
      new CustomS3StorageService(
        //        StorageConfig(storageType, AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config"), Option(storageEndpoint))
        StorageConfig(storageType, storageKey, storageSecret, Option(storageEndpoint))
      )
    } else {
      StorageServiceFactory.getStorageService(
        StorageConfig(storageType, AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config"))
      )
    }
    storageService
  }


  def removeFile(path: String)(implicit spark: SparkSession): Unit = {
    val successFile = new Path(path)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(successFile)) {
      fs.delete(successFile, true)
    }

  }

  def renameCSV(ids: Array[String], path: String)(): Unit = {
    for (id <- ids) {
      val tmpcsv = new File(path + s"mdoid=${id}")
      val customized = new File(path + s"mdoid=${id}/${id}.csv")

      val tempCsvFileOpt = tmpcsv.listFiles().find(file => file.getName.startsWith("part-"))

      if (tempCsvFileOpt != None) {
        val finalFile = tempCsvFileOpt.get
        finalFile.renameTo(customized)
      }
    }
  }

}

