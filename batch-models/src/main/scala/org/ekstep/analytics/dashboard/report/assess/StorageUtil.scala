package org.ekstep.analytics.dashboard.report.assess

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.storage.CustomS3StorageService
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

object StorageUtil {

  def getStorageService(config: JobConfig): BaseStorageService = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint_with_protocol")
    val storageType = "s3"
    val storageKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String]
    val storageSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
    val storageService = if ("s3".equalsIgnoreCase(storageType) && !"".equalsIgnoreCase(storageEndpoint)) {
      new CustomS3StorageService(
        StorageConfig(storageType, AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config"), Option(storageEndpoint))
      )
    } else {
      StorageServiceFactory.getStorageService(
        StorageConfig(storageType, AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config"))
      )
    }
    storageService
  }

}
