package org.ekstep.analytics.dashboard.report.assessment

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger

object UserAssessmentJob extends optional.Application with IJob{

  implicit val className = "org.ekstep.analytics.dashboard.report.assessment.UserAssessmentJob"
  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit ={
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", "config", UserAssessmentModel)
    JobLogger.log("Job Completed.")
  }
}