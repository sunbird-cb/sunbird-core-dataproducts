package org.ekstep.analytics.dashboard.report.assess_new

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object AllAssessmentJob extends optional.Application with IJob {
  
    implicit val className = "org.ekstep.analytics.dashboard.report.assess.AllAssessmentJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, AllAssessmentModel);
        JobLogger.log("Job Completed.")
    }
}