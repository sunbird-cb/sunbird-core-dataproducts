package org.ekstep.analytics.dashboard.report.acbp

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object UserACBPReportJob extends optional.Application with IJob{
  implicit val className = "org.ekstep.analytics.dashboard.report.acbp.UserACBPReportJob"
  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit ={
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, UserACBPReportModel)
    JobLogger.log("Job Completed.")
  }
}

