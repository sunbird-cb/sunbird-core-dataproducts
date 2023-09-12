package org.ekstep.analytics.dashboard.report.blendedprogram

import org.apache.spark.SparkContext
import org.ekstep.analytics.dashboard.report.blendedprogram.bpUserAssessmentModel
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object bpUserAssessmentJob extends optional.Application with IJob {
  
    implicit val className = "org.ekstep.analytics.dashboard.report.blendedprogram.bpUserAssessmentModel"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, bpUserAssessmentModel);
        JobLogger.log("Job Completed.")
    }
}