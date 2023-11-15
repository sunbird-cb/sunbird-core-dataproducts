package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestWorkFlowSummarizer2 extends SparkSpec(null) {
  
    "WorkFlowSummarizer" should "execute WorkFlowSummarizer job and won't throw any Exception" in {

        val config = JobConfig(
            Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("/home/shobhit/projects/github/sunbird-cb/sunbird-core-dataproducts/batch-models/src/test/resources/workflow-summary/test-data1.log"))))),
            null,
            null,
            "org.ekstep.analytics.model.WorkFlowSummary",
            None,
            Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
            Option(10),
            Option("TestWorkFlowSummarizer"),
            Option(true)
        )
        WorkFlowSummarizer2.main(JSONUtils.serialize(config))(Option(sc));
    }
}