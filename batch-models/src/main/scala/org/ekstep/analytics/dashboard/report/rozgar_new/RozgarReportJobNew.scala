package org.ekstep.analytics.dashboard.report.rozgar_new

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object RozgarReportJobNew extends optional.Application with IJob{

  implicit val className = "org.ekstep.analytics.dashboard.report.rozgar_new.RozgarReportJobNew"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, RozgarReportModelNew);
    JobLogger.log("Job Completed.")
  }

}
