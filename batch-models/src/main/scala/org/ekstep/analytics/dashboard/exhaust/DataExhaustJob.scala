package org.ekstep.analytics.dashboard.exhaust

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object DataExhaustJob extends optional.Application with IJob {
  
    implicit val className = "org.ekstep.analytics.dashboard.DataExhaustJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, DataExhaustModel);
        JobLogger.log("Job Completed.")
    }
}