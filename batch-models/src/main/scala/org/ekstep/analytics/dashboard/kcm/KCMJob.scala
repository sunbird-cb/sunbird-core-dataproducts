package org.ekstep.analytics.dashboard.kcm

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object KCMJob extends optional.Application with IJob{
  implicit val className = "org.ekstep.analytics.dashboard..report.kcm.KCMJob"

  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit ={
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing KCM Job")
    JobDriver.run("batch", config, KCMModel)
    JobLogger.log("Job Completed.")
  }
}
