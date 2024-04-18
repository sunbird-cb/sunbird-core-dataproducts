package org.ekstep.analytics.dashboard.report.observation.question

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger

object ObservationQuestionReportJob extends optional.Application with IJob {
  implicit val className = "org.ekstep.analytics.dashboard.report.observation.question.ObservationQuestionReportJob"

  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, ObservationQuestionReportModel)
    JobLogger.log("Job Completed.")
  }

}
