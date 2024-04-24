package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.{FrameworkContext, StorageConfig}
import DashboardUtil._
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.{CompressionLevel, CompressionMethod}
import org.apache.commons.io.FileUtils

import java.io.{File, Serializable}
import java.util
import scala.collection.mutable.ListBuffer


object DataUtil extends Serializable {

  /*
  organizes schema def at one place
   */
  object Schema extends Serializable {

    /* schema definitions for user profile details */
    val profileCompetencySchema: StructType = StructType(Seq(
      StructField("id",  StringType, nullable = true),
      StructField("name",  StringType, nullable = true),
      StructField("status",  StringType, nullable = true),
      StructField("competencyType",  StringType, nullable = true),
      StructField("competencySelfAttestedLevel",  StringType, nullable = true), // this is sometimes an int other times a string
      StructField("competencySelfAttestedLevelValue",  StringType, nullable = true)
    ))
    val professionalDetailsSchema: StructType = StructType(Seq(
      StructField("designation", StringType, nullable = true),
      StructField("group", StringType, nullable = true)
    ))
    val personalDetailsSchema: StructType = StructType(Seq(
      StructField("phoneVerified", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("mobile", StringType, nullable = true),
      StructField("primaryEmail", StringType, nullable = true)
    ))
    val additionalPropertiesSchema: StructType = StructType(Seq(
      StructField("tag", ArrayType(StringType), nullable = true),
      StructField("externalSystemId", StringType, nullable = true),
      StructField("externalSystem", StringType, nullable = true)
    ))
    def makeProfileDetailsSchema(competencies: Boolean = false, additionalProperties: Boolean = false, professionalDetails: Boolean = false): StructType = {
      val fields = ListBuffer(
        StructField("verifiedKarmayogi", BooleanType, nullable = true),
        StructField("mandatoryFieldsExists", BooleanType, nullable = true),
        StructField("profileImageUrl", StringType, nullable = true),
        StructField("personalDetails", personalDetailsSchema, nullable = true)
      )
      if (competencies) {
        fields.append(StructField("competencies", ArrayType(profileCompetencySchema), nullable = true))
      }
      if (additionalProperties) {
        fields.append(StructField("additionalProperties", additionalPropertiesSchema, nullable = true))
        fields.append(StructField("additionalPropertis", additionalPropertiesSchema, nullable = true))
      }
      if (professionalDetails) {
        fields.append(StructField("professionalDetails", ArrayType(professionalDetailsSchema), nullable = true))
      }
      StructType(fields)
    }

    /* schema definitions for competencies */
    val courseCompetenciesSchema: ArrayType = ArrayType(StructType(Seq(
      StructField("id",  StringType, nullable = true),
      StructField("name",  StringType, nullable = true),
      // StructField("description",  StringType, nullable = true),
      // StructField("source",  StringType, nullable = true),
      StructField("competencyType",  StringType, nullable = true),
      // StructField("competencyArea",  StringType, nullable = true),
      // StructField("selectedLevelId",  StringType, nullable = true),
      // StructField("selectedLevelName",  StringType, nullable = true),
      // StructField("selectedLevelSource",  StringType, nullable = true),
      StructField("selectedLevelLevel",  StringType, nullable = true)
      //StructField("selectedLevelDescription",  StringType, nullable = true)
    )))
    val expectedCompetencySchema: StructType = StructType(Seq(
      StructField("orgID",  StringType, nullable = true),
      StructField("workOrderID",  StringType, nullable = true),
      StructField("userID",  StringType, nullable = true),
      StructField("competencyID",  StringType, nullable = true),
      StructField("expectedCompetencyLevel",  IntegerType, nullable = true)
    ))
    val activeUsersSchema: StructType = StructType(Seq(
      StructField("orgID",  StringType, nullable = true),
      StructField("activeCount",  LongType, nullable = true)
    ))
    val monthlyActiveUsersSchema: StructType = StructType(Seq(
      StructField("DAUOutput",  LongType, nullable = true)
    ))
    val timeSpentSchema: StructType = StructType(Seq(
      StructField("orgID",  StringType, nullable = true),
      StructField("timeSpent",  FloatType, nullable = true)
    ))
    val fracCompetencySchema: StructType = StructType(Seq(
      StructField("competencyID",  StringType, nullable = true),
      StructField("competencyName",  StringType, nullable = true),
      StructField("competencyStatus",  StringType, nullable = true)
    ))

    /* schema definitions for content hierarchy table */
    def makeHierarchyChildSchema(children: Boolean = false): StructType = {
      val fields = ListBuffer(
        StructField("identifier", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("channel", StringType, nullable = true),
        StructField("duration", StringType, nullable = true),
        StructField("primaryCategory", StringType, nullable = true),
        StructField("leafNodesCount", IntegerType, nullable = true),
        StructField("contentType", StringType, nullable = true),
        StructField("objectType", StringType, nullable = true),
        StructField("showTimer", StringType, nullable = true),
        StructField("allowSkip", StringType, nullable = true)
      )
      if (children) {
        fields.append(StructField("children", ArrayType(makeHierarchyChildSchema()), nullable = true))
      }
      StructType(fields)
    }

    def makeHierarchySchema(children: Boolean = false, competencies: Boolean = false, l2Children: Boolean = false): StructType = {
      val fields = ListBuffer(
        StructField("name", StringType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("reviewStatus", StringType, nullable = true),
        StructField("channel", StringType, nullable = true),
        StructField("duration", StringType, nullable = true),
        StructField("primaryCategory", StringType, nullable = true),
        StructField("leafNodesCount", IntegerType, nullable = true),
        StructField("leafNodes", ArrayType(StringType), nullable = true),
        StructField("publish_type", StringType, nullable = true),
        StructField("isExternal", BooleanType, nullable = true),
        StructField("contentType", StringType, nullable = true),
        StructField("objectType", StringType, nullable = true),
        StructField("userConsent", StringType, nullable = true),
        StructField("visibility", StringType, nullable = true),
        StructField("createdOn", StringType, nullable = true),
        StructField("lastUpdatedOn", StringType, nullable = true),
        StructField("lastPublishedOn", StringType, nullable = true),
        StructField("lastSubmittedOn", StringType, nullable = true),
        StructField("lastStatusChangedOn", StringType, nullable = true),
        StructField("createdFor", ArrayType(StringType), nullable = true)
      )
      if (children) {
        fields.append(StructField("children", ArrayType(makeHierarchyChildSchema(l2Children)), nullable = true))
      }
      if (competencies) {
        fields.append(StructField("competencies_v3", StringType, nullable = true))
      }
      StructType(fields)
    }

    /* assessment related schema */
    val assessmentReadResponseSchema: StructType = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("objectType", StringType, nullable = true),
      StructField("version", IntegerType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("totalQuestions", IntegerType, nullable = true),
      StructField("maxQuestions", IntegerType, nullable = true),
      StructField("expectedDuration", IntegerType, nullable = true),
      StructField("primaryCategory", StringType, nullable = true),
      StructField("maxAssessmentRetakeAttempts", IntegerType, nullable = true)
    ))
    val submitAssessmentRequestSchema: StructType = StructType(Seq(
      StructField("courseId", StringType, nullable = false),
      StructField("batchId", StringType, nullable = false),
      StructField("primaryCategory", StringType, nullable = false),
      StructField("isAssessment", BooleanType, nullable = false),
      StructField("timeLimit", IntegerType, nullable = false)
    ))
    val submitAssessmentResponseSchema: StructType = StructType(Seq(
      StructField("result", FloatType, nullable = false),
      StructField("total", IntegerType, nullable = false),
      StructField("blank", IntegerType, nullable = false),
      StructField("correct", IntegerType, nullable = false),
      StructField("incorrect", IntegerType, nullable = false),
      StructField("pass", BooleanType, nullable = false),
      StructField("overallResult", FloatType, nullable = false),
      StructField("passPercentage", FloatType, nullable = false)
    ))

    /* batch attrs schema */
    val batchAttrsSessionDetailsV2FacilatorDetailsSchema: StructType = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("email", StringType, nullable = true)
    ))
    val batchAttrsSessionDetailsV2Schema: StructType = StructType(Seq(
      StructField("sessionId", StringType, nullable = true),
      StructField("sessionType", StringType, nullable = true),
      StructField("sessionDuration", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("startDate", StringType, nullable = true),
      StructField("startTime", StringType, nullable = true),
      StructField("endTime", StringType, nullable = true),
      StructField("facilatorDetails", ArrayType(batchAttrsSessionDetailsV2FacilatorDetailsSchema), nullable = true)
    ))
    val batchAttrsSchema: StructType = StructType(Seq(
      StructField("batchLocationDetails", StringType, nullable = true),
      StructField("latlong", StringType, nullable = true),
      StructField("currentBatchSize", StringType, nullable = true),
      StructField("sessionDetails_v2", ArrayType(batchAttrsSessionDetailsV2Schema), nullable = true)
    ))

    /* telemetry related schema */
    val loggedInMobileUserSchema: StructType = StructType(Seq(
      StructField("userID", StringType, nullable = true),
      StructField("userLoginFromMobile", BooleanType, nullable = true)
    ))
    val loggedInWebUserSchema: StructType = StructType(Seq(
      StructField("userID", StringType, nullable = true),
      StructField("userLoginFromWeb", BooleanType, nullable = true)
    ))
    val userActualTimeSpentLearningSchema: StructType = StructType(Seq(
      StructField("userID", StringType, nullable = true),
      StructField("userActualTimeSpentLearning", FloatType, nullable = true)
    ))
    val usersPlatformEngagementSchema: StructType = StructType(Seq(
      StructField("userid", StringType, nullable = true),
      StructField("platformEngagementTime", FloatType, nullable = true),
      StructField("sessionCount", IntegerType, nullable = true)
    ))
    val npsUserIds: StructType = StructType(Seq(
      StructField("userid", StringType, nullable = true)
    ))

    val totalLearningHoursSchema: StructType = StructType(Seq(
      StructField("userOrgID", StringType),
      StructField("totalLearningHours", StringType)
    ))
    val cbplanDraftDataSchema: StructType = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("assignmentType", StringType, nullable = true),
      StructField("assignmentTypeInfo", ArrayType(StringType), nullable = true),
      StructField("endDate", StringType, nullable = true),
      StructField("contentList", ArrayType(StringType), nullable = true)
    ))

    val solutionIdDataSchema: StructType = StructType(Seq(
      StructField("createdBy", StringType, nullable = true),
      StructField("user_type", StringType, nullable = true),
      StructField("user_subtype", StringType, nullable = true),
      StructField("state_name", StringType, nullable = true),
      StructField("district_name", StringType, nullable = true),
      StructField("block_name", StringType, nullable = true),
      StructField("school_code", StringType, nullable = true),
      StructField("school_name", StringType, nullable = true),
      StructField("board_name", StringType, nullable = true),
      StructField("organisation_name", StringType, nullable = true),
      StructField("programName", StringType, nullable = true),
      StructField("programExternalId", StringType, nullable = true),
      StructField("solutionName", StringType, nullable = true),
      StructField("solutionExternalId", StringType, nullable = true),
      StructField("surveySubmissionId", StringType, nullable = true),
      StructField("questionExternalId", StringType, nullable = true),
      StructField("questionName", StringType, nullable = true),
      StructField("questionResponseLabel", StringType, nullable = true),
      StructField("evidences", StringType, nullable = true),
      StructField("remarks", StringType, nullable = true)
    ))

    val uniqueSolutionIdsDataSchema: StructType = StructType(Seq(
      StructField("solutionIds", StringType, nullable = true)
    ))

    val solutionsEndDateDataSchema: StructType = StructType(Seq(
      StructField("_id", StringType, nullable = true),
      StructField("endDate", DateType, nullable = true)
    ))

    val observationStatusCompletedDataSchema: StructType = StructType(Seq(
      StructField("completedAt", StringType, nullable = true),
      StructField("observationSubmissionId", StringType, nullable = true)
    ))

    val observationStatusInProgressDataSchema: StructType = StructType(Seq(
      StructField("inprogressAt", StringType, nullable = true),
      StructField("observationSubmissionId", StringType, nullable = true)
    ))

  }

  def elasticSearchCourseProgramDataFrame(primaryCategories: Seq[String])(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    cache.load("esContent").filter(col("primaryCategory").isin(primaryCategories:_*))
  }

  def fracCompetencyAPI(host: String): String = {
    val url = s"https://${host}/graphql"
    val requestBody = """{"operationName":"filterCompetencies","variables":{"cod":[],"competencyType":[],"competencyArea":[],"competencySector":[]},"query":"query filterCompetencies($cod: [String], $competencyType: [String], $competencyArea: [String], $competencySector: [String]) {\n  getAllCompetencies(\n    cod: $cod\n    competencyType: $competencyType\n    competencyArea: $competencyArea\n    competencySector: $competencySector\n  ) {\n    name\n    id\n    description\n    status\n    source\n    additionalProperties {\n      cod\n      competencyType\n      competencyArea\n      competencySector\n      __typename\n    }\n    __typename\n  }\n}\n"}"""
    api("POST", url, requestBody)
  }

  def fracCompetencyDFOption(host: String)(implicit spark: SparkSession): Option[DataFrame] = {
    var result = fracCompetencyAPI(host)
    result = result.trim()
    // return empty data frame if result is an empty string
    if (result == "") {
      println(s"ERROR: fracCompetencyAPI returned empty string")
      return None
    }
    val df = dataFrameFromJSONString(result).persist(StorageLevel.MEMORY_ONLY)  // parse json string
    if (df.isEmpty) {
      println(s"ERROR: fracCompetencyAPI json parse result is empty")
      return None
    }
    // return empty data frame if there is an `errors` field in the json
    if (hasColumn(df, "errors")) {
      println(s"ERROR: fracCompetencyAPI returned error response, response=${result}")
      return None
    }
    // now that error handling is done, proceed with business as usual
    Some(df)
  }

  def timestampStringToLong(df: DataFrame, cols: Seq[String], format: String = "yyyy-MM-dd HH:mm:ss:SSSZ"): DataFrame = {
    var resDF = df
    cols.foreach(c => {
      resDF = resDF.withColumn(c, to_timestamp(col(c), format))
        .withColumn(c, col(c).cast("long"))
    })
    resDF
  }

  /**
   * org data from cassandra TODO
   * @return DataFrame(orgID, orgName, orgStatus, orgCreatedDate, orgType, orgSubType)
   */
  def orgDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var orgDF = cache.load("org")
      .select(
        col("id").alias("orgID"),
        col("orgname").alias("orgName"),
        col("status").alias("orgStatus"),
        col("createddate").alias("orgCreatedDate"),
        col("organisationtype").alias("orgType"),
        col("organisationsubtype").alias("orgSubType")
      ).na.fill("", Seq("orgName"))

    orgDF = timestampStringToLong(orgDF, Seq("orgCreatedDate"))

    show(orgDF, "orgDataFrame")

    orgDF
  }

  /**
   * user data from cassandra TODO
   * @return DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus, userCreatedTimestamp, userUpdatedTimestamp,
   *         userVerified, userMandatoryFieldsExists, userPhoneVerified)
   */
  def userDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val profileDetailsSchema = Schema.makeProfileDetailsSchema(additionalProperties = true, professionalDetails = true)
    var userDF = cache.load("user")
      .select(
        col("id").alias("userID"),
        col("firstname").alias("firstName"),
        col("lastname").alias("lastName"),
        col("maskedemail").alias("maskedEmail"),
        col("maskedphone").alias("maskedPhone"),
        col("rootorgid").alias("userOrgID"),
        col("status").alias("userStatus"),
        col("profiledetails").alias("userProfileDetails"),
        col("createddate").alias("userCreatedTimestamp"),
        col("updateddate").alias("userUpdatedTimestamp"),
        col("createdby").alias("userCreatedBy")
      )
      .na.fill("", Seq("userOrgID", "firstName", "lastName"))
      .na.fill("{}", Seq("userProfileDetails"))
      .withColumn("profileDetails", from_json(col("userProfileDetails"), profileDetailsSchema))
      .withColumn("personalDetails", col("profileDetails.personalDetails"))
      .withColumn("professionalDetails", explode_outer(col("profileDetails.professionalDetails")))
      .withColumn("userVerified", when(col("profileDetails.verifiedKarmayogi").isNull, false).otherwise(col("profileDetails.verifiedKarmayogi")))
      .withColumn("userMandatoryFieldsExists", col("profileDetails.mandatoryFieldsExists"))
      .withColumn("userProfileImgUrl", col("profileDetails.profileImageUrl"))
      .withColumn("userPhoneVerified", expr("LOWER(personalDetails.phoneVerified) = 'true'"))
      .withColumn("fullName", concat_ws(" ", col("firstName"), col("lastName")))

    userDF = userDF
      .withColumn("additionalProperties",
        if (userDF.columns.contains("profileDetails.additionalPropertis")) {
          col("profileDetails.additionalPropertis")
        } else {
          col("profileDetails.additionalProperties")
        })
      .drop("profileDetails", "userProfileDetails")

    userDF = timestampStringToLong(userDF, Seq("userCreatedTimestamp", "userUpdatedTimestamp"))
    show(userDF, "userDataFrame")

    userDF
  }

  /**
   * de-normalize user data with org data
   *
   * @param orgDF DataFrame(orgID, orgName, orgStatus, orgCreatedDate, orgType, orgSubType)
   * @param userDF DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus, userCreatedTimestamp, userUpdatedTimestamp,
   *               userVerified, userMandatoryFieldsExists, userPhoneVerified)
   * @return DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userCreatedTimestamp, userUpdatedTimestamp,
   *         userVerified, userMandatoryFieldsExists, userPhoneVerified,
   *         userOrgID, userOrgName, userOrgStatus, userOrgCreatedDate, userOrgType, userOrgSubType)
   */
  def userOrgDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    val joinOrgDF = orgDF.select(
      col("orgID").alias("userOrgID"),
      col("orgName").alias("userOrgName"),
      col("orgStatus").alias("userOrgStatus"),
      col("orgCreatedDate").alias("userOrgCreatedDate"),
      col("orgType").alias("userOrgType"),
      col("orgSubType").alias("userOrgSubType")
    )

    val df = userDF.join(joinOrgDF, Seq("userOrgID"), "left")
    show(df, "userOrgDataFrame")

    df
  }

  /**
   *
   * @return DataFrame(userID, role)
   */
  def roleDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val roleDF = cache.load("role")
      .select(
        col("userid").alias("userID"),
        col("role").alias("role")
      )
    show(roleDF, "User Role DataFrame")

    roleDF
  }

  /**
   *
   * @param userOrgDF DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus)
   * @param roleDF DataFrame(userID, role)
   * @return DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   */
  def userOrgRoleDataFrame(userOrgDF: DataFrame, roleDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    // userID, userStatus, orgID, orgName, orgStatus, role
    val joinUserOrgDF = userOrgDF.select(
      col("userID"), col("userStatus"),
      col("userOrgID"), col("userOrgName"), col("userOrgStatus")
    )
    val userOrgRoleDF = joinUserOrgDF.join(roleDF, Seq("userID"), "left")
    show(userOrgRoleDF)

    userOrgRoleDF
  }

  /**
   *
   * @param userOrgRoleDF DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   * @return DataFrame(role, count)
   */
  def roleCountDataFrame(userOrgRoleDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val roleCountDF = userOrgRoleDF
      .where(expr("userStatus=1 AND userOrgStatus=1"))
      .groupBy("role").agg(countDistinct("userID").alias("count"))
    show(roleCountDF)

    roleCountDF
  }

  /**
   *
   * @param userOrgRoleDF DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   * @return DataFrame(orgID, orgName, role, count)
   */
  def orgRoleCountDataFrame(userOrgRoleDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val orgRoleCount = userOrgRoleDF
      .where(expr("userStatus=1 AND userOrgStatus=1"))
      .groupBy("userOrgID", "userOrgName", "role")
      .agg(countDistinct("userID").alias("count"))
      .select(
        col("userOrgID").alias("orgID"),
        col("userOrgName").alias("orgName"),
        col("role"), col("count")
      )

    show(orgRoleCount)

    orgRoleCount
  }

  /**
   *
   * @param orgDF DataFrame(orgID, orgName, orgStatus)
   * @param userDF DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus)
   * @return DataFrame(orgID, orgName, registeredCount, totalCount)
   */
  def orgUserCountDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val orgUserDF = orgDF.join(userDF.withColumnRenamed("userOrgID", "orgID").filter(col("orgID").isNotNull), Seq("orgID"), "left")
      .where(expr("userStatus=1 AND orgStatus=1"))
    show(orgUserDF, "Org User DataFrame")

    val orgUserCountDF = orgUserDF.groupBy("orgID", "orgName").agg(expr("count(userID)").alias("registeredCount"))
      .withColumn("totalCount", lit(10000))
    show(orgUserCountDF, "Org User Count DataFrame")

    orgUserCountDF
  }

  /**
   *
   * @param orgUserCountDF DataFrame(orgID, orgName, registeredCount, totalCount)
   * @return registered user count map, total  user count map, and orgID-orgName map
   */
  def getOrgUserMaps(orgUserCountDF: DataFrame): (util.Map[String, String], util.Map[String, String], util.Map[String, String]) = {
    val orgRegisteredUserCountMap = new util.HashMap[String, String]()
    val orgTotalUserCountMap = new util.HashMap[String, String]()
    val orgNameMap = new util.HashMap[String, String]()

    orgUserCountDF.collect().foreach(row => {
      val orgID = row.getAs[String]("orgID")
      orgRegisteredUserCountMap.put(orgID, row.getAs[Long]("registeredCount").toString)
      orgTotalUserCountMap.put(orgID, row.getAs[Long]("totalCount").toString)
      orgNameMap.put(orgID, row.getAs[String]("orgName"))
    })

    (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap)
  }


  /**
   * content from elastic search api
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, lastPublishedOn)
   */
  def contentESDataFrame(primaryCategories: Seq[String], prefix: String = "course")(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = elasticSearchCourseProgramDataFrame(primaryCategories)

    // now that error handling is done, proceed with business as usual
    df = df
      .withColumn(s"${prefix}OrgID", explode_outer(col("createdFor")))
      .select(
        col("identifier").alias(s"${prefix}ID"),
        col("primaryCategory").alias(s"${prefix}Category"),
        col("name").alias(s"${prefix}Name"),
        col("status").alias(s"${prefix}Status"),
        col("reviewStatus").alias(s"${prefix}ReviewStatus"),
        col("channel").alias(s"${prefix}Channel"),
        col("lastPublishedOn").alias(s"${prefix}LastPublishedOn"),
        col("duration").cast(FloatType).alias(s"${prefix}Duration"),
        col("leafNodesCount").alias(s"${prefix}ResourceCount"),
        col("lastStatusChangedOn").alias(s"${prefix}LastStatusChangedOn"),
        col("programDirectorName").alias(s"${prefix}ProgramDirectorName"),
        col(s"${prefix}OrgID")
      )

    df = df.dropDuplicates(s"${prefix}ID", s"${prefix}Category")
    df = df
      .na.fill(0.0, Seq(s"${prefix}Duration"))
      .na.fill(0, Seq(s"${prefix}ResourceCount"))
      .durationFormat(s"${prefix}Duration")

    show(df, "contentESDataFrame")
    df
  }


  /**
   * All courses/programs from elastic search api
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, lastPublishedOn)
   */
  def allCourseProgramESDataFrame(primaryCategories: Seq[String])(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = elasticSearchCourseProgramDataFrame(primaryCategories)

    // now that error handling is done, proceed with business as usual
    df = df
      .withColumn("courseOrgID", explode_outer(col("createdFor")))
      .select(
        col("identifier").alias("courseID"),
        col("primaryCategory").alias("category"),
        col("name").alias("courseName"),
        col("status").alias("courseStatus"),
        col("reviewStatus").alias("courseReviewStatus"),
        col("channel").alias("courseChannel"),
        col("lastPublishedOn").alias("courseLastPublishedOn"),
        col("duration").cast(FloatType).alias("courseDuration"),
        col("leafNodesCount").alias("courseResourceCount"),
        col("lastStatusChangedOn").alias("lastStatusChangedOn"),
        col("courseOrgID"),
        col("competencies_v5.competencyAreaId"),
        col("competencies_v5.competencyThemeId"),
        col("competencies_v5.competencySubThemeId")

      )

    df = df.dropDuplicates("courseID", "category")
    df = df
      .na.fill(0.0, Seq("courseDuration"))
      .na.fill(0, Seq("courseResourceCount"))
    show(df, "allCourseProgramESDataFrame")
    df
  }

  /**
   * All Stand-alone Assessments from elastic search api
   * @return DataFrame(assessID, assessCategory, assessName, assessStatus, assessReviewStatus, assessOrgID, assessDuration,
   *         assessChildCount)
   */
  def assessmentESDataFrame(primaryCategories: Seq[String])(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    var df = elasticSearchCourseProgramDataFrame(primaryCategories)

    // now that error handling is done, proceed with business as usual
    df = df
      .withColumn("assessOrgID", explode_outer(col("createdFor")))
      .select(
        col("identifier").alias("assessID"),
        col("primaryCategory").alias("assessCategory"),
        col("name").alias("assessName"),
        col("status").alias("assessStatus"),
        col("reviewStatus").alias("assessReviewStatus"),
        col("channel").alias("assessChannel"),
        col("duration").cast(FloatType).alias("assessDuration"),
        col("leafNodesCount").alias("assessChildCount"),
        col("lastPublishedOn").alias("assessLastPublishedOn"),
        col("assessOrgID")
      )

    df = df.dropDuplicates("assessID", "assessCategory")
    df = df.na.fill(0.0, Seq("assessDuration")).na.fill(0, Seq("assessChildCount"))


    show(df, "assessmentESDataFrame")
    df
  }

  def addAssessOrgDetails(assessmentDF: DataFrame, orgDF: DataFrame): DataFrame = {
    val assessOrgDF = orgDF.select(
      col("orgID").alias("assessOrgID"),
      col("orgName").alias("assessOrgName"),
      col("orgStatus").alias("assessOrgStatus")
    )
    val df = assessmentDF.join(assessOrgDF, Seq("assessOrgID"), "left")

    show(df, "addAssessOrgDetails")
    df
  }

  /**
   *
   * @param assessmentDF
   * @param hierarchyDF
   * @return DataFrame(assessID, assessCategory, assessName, assessStatus, assessReviewStatus, assessOrgID,
   *         assessOrgName, assessOrgStatus, assessDuration, assessChildCount, children,
   *         assessPublishType, assessIsExternal, assessContentType, assessObjectType, assessUserConsent,
   *         assessVisibility, assessCreatedOn, assessLastUpdatedOn, assessLastPublishedOn, assessLastSubmittedOn)
   */
  def assessWithHierarchyDataFrame(assessmentDF: DataFrame, hierarchyDF: DataFrame, orgDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    var df = addHierarchyColumn(assessmentDF, hierarchyDF, "assessID", "data", children = true)

    df = addAssessOrgDetails(df, orgDF)

    df = df
      .withColumn("children", col("data.children"))
      .withColumn("assessPublishType", col("data.publish_type"))
      .withColumn("assessIsExternal", col("data.isExternal"))
      .withColumn("assessContentType", col("data.contentType"))
      .withColumn("assessObjectType", col("data.objectType"))
      .withColumn("assessUserConsent", col("data.userConsent"))
      .withColumn("assessVisibility", col("data.visibility"))
      .withColumn("assessCreatedOn", col("data.createdOn"))
      .withColumn("assessLastUpdatedOn", col("data.lastUpdatedOn"))
      .withColumn("assessLastSubmittedOn", col("data.lastSubmittedOn"))
      .drop("data")

    df = timestampStringToLong(df,
      Seq("assessCreatedOn", "assessLastUpdatedOn", "assessLastPublishedOn", "assessLastSubmittedOn"),
      "yyyy-MM-dd'T'HH:mm:ss")

    show(df, "assessWithHierarchyDataFrame")
    df
  }

  /**
   * Attach org info to course/program data
   * @param courseDF DataFrame(courseOrgID, ...)
   * @param orgDF DataFrame(orgID, orgName, orgStatus)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus)
   */
  def addCourseOrgDetails(courseDF: DataFrame, orgDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    val joinOrgDF = orgDF.select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )
    val df = courseDF.join(joinOrgDF, Seq("courseOrgID"), "left")

    show(df, "addCourseOrgDetails")
    df
  }

  def contentHierarchyDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    cache.load("hierarchy").select(col("identifier"), col("hierarchy"))
  }

  /**
   * Adds hierarchy column, parses json and adds it as a column
   * @param df dataframe to add column to
   * @param hierarchyDF hierarchy table dataframe
   * @param idCol
   * @param asCol
   * @param children
   * @param competencies
   * @return
   */
  def addHierarchyColumn(df: DataFrame, hierarchyDF: DataFrame, idCol: String, asCol: String,
                         children: Boolean = false, competencies: Boolean = false, l2Children: Boolean = false
                        )(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val hierarchySchema = Schema.makeHierarchySchema(children, competencies, l2Children)
    df.join(hierarchyDF, df.col(idCol) === hierarchyDF.col("identifier"), "left")
      .na.fill("{}", Seq("hierarchy"))
      .withColumn(asCol, from_json(col("hierarchy"), hierarchySchema))
      .drop("hierarchy")
  }

  /**
   * course details with competencies json from cassandra dev_hierarchy_store:content_hierarchy
   * @param allCourseProgramESDF
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   */
  def allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramESDF: DataFrame, hierarchyDF: DataFrame, orgDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = addHierarchyColumn(allCourseProgramESDF, hierarchyDF, "courseID", "data", competencies = true)

    df = df
      .withColumn("competenciesJson", col("data.competencies_v3"))

    //      .withColumn("courseName", col("data.name"))
    //      .withColumn("courseStatus", col("data.status"))
    //      .withColumn("courseDuration", col("data.duration").cast(FloatType))
    //      .withColumn("category", col("data.primaryCategory"))
    //      .withColumn("courseReviewStatus", col("data.reviewStatus"))
    //      .withColumn("courseResourceCount", col("data.leafNodesCount"))

    df = addCourseOrgDetails(df, orgDF)

    df = df
      .na.fill(0.0, Seq("courseDuration"))
      .na.fill(0, Seq("courseResourceCount"))
      .drop("data")

    show(df, "allCourseProgramDetailsWithCompetenciesJsonDataFrame() = (courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson, lastPublishedOn)")
    df
  }

  def liveCourseDataFrame(allCourseProgramDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = allCourseProgramDF.where(expr("category='Course' and courseStatus='Live'")).select(col("courseID").alias("id")).distinct()

    show(df)
    df
  }

  /**
   * course details without competencies json
   * @param allCourseProgramDetailsWithCompDF course details with competencies json
   *                                          DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                          courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   */
  def allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF: DataFrame): DataFrame = {
    val df = allCourseProgramDetailsWithCompDF.drop("competenciesJson")

    show(df)
    df
  }

  def getOrgUserDataFrames()(implicit spark: SparkSession, conf: DashboardConfig): (DataFrame, DataFrame, DataFrame) = {
    // obtain and save user org data
    val orgDF = orgDataFrame()
    val userDF = userDataFrame()
    val userOrgDF = userOrgDataFrame(orgDF, userDF)
    // validate userDF and userOrgDF counts
    validate({userDF.count()}, {userOrgDF.count()}, "userDF.count() should equal userOrgDF.count()")

    (orgDF, userDF, userOrgDF)
  }

  def validatePrimaryCategories(primaryCategories: Seq[String]): Unit = {
    val allowedCategories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment","Curated Program")
    val notAllowed = primaryCategories.toSet.diff(allowedCategories.toSet)
    if (notAllowed.nonEmpty) {
      throw new Exception(s"Category not allowed: ${notAllowed.mkString(", ")}")
    }
  }

  def contentWithOrgDetailsDataFrame(orgDF: DataFrame, primaryCategories: Seq[String])(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    validatePrimaryCategories(primaryCategories)

    val allCourseProgramDetailsDF = allCourseProgramESDataFrame(primaryCategories)
    val courseOrgDF = orgDF.select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )
    val df = allCourseProgramDetailsDF
      .join(courseOrgDF, Seq("courseOrgID"), "left")

    show(df)
    df
  }

  def contentDataFrames(orgDF: DataFrame, primaryCategories: Seq[String] = Seq("Course", "Program"), runValidation: Boolean = true)(implicit spark: SparkSession, conf: DashboardConfig): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    validatePrimaryCategories(primaryCategories)

    val hierarchyDF = contentHierarchyDataFrame()
    val allCourseProgramESDF = allCourseProgramESDataFrame(primaryCategories)
    val allCourseProgramDetailsWithCompDF = allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramESDF, hierarchyDF, orgDF)
    val allCourseProgramDetailsDF = allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF)
    val courseRatingDF = courseRatingSummaryDataFrame()
    val allCourseProgramDetailsWithRatingDF = allCourseProgramDetailsWithRatingDataFrame(allCourseProgramDetailsDF, courseRatingDF)

    if (runValidation) {
      // validate that no rows are getting dropped b/w allCourseProgramESDF and allCourseProgramDetailsWithRatingDF
      validate({allCourseProgramESDF.count()}, {allCourseProgramDetailsWithRatingDF.count()}, "ES course count should equal final DF with rating count")
      // validate that # of rows with ratingSum > 0 in the final DF is equal to # of rows in courseRatingDF from cassandra
      val pcLowerStr = primaryCategories.map(c => s"'${c.toLowerCase()}'").mkString(", ")
      validate(
        {courseRatingDF.where(expr(s"categoryLower IN (${pcLowerStr}) AND ratingSum > 0")).count()},
        {allCourseProgramDetailsWithRatingDF.where(expr(s"LOWER(category) IN (${pcLowerStr}) AND ratingSum > 0")).count()},
        "number of ratings in cassandra table for courses and programs with ratingSum > 0 should equal those in final druid datasource")
      // validate rating data, sanity check
      Seq(1, 2, 3, 4, 5).foreach(i => {
        validate(
          {courseRatingDF.where(expr(s"categoryLower IN (${pcLowerStr}) AND ratingAverage <= ${i}")).count()},
          {allCourseProgramDetailsWithRatingDF.where(expr(s"LOWER(category) IN (${pcLowerStr}) AND ratingAverage <= ${i}")).count()},
          s"Rating data row count for courses and programs should equal final DF for ratingAverage <= ${i}"
        )
      })
    }

    (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF, allCourseProgramDetailsWithRatingDF)
  }

  /**
   * course competency mapping data from cassandra dev_hierarchy_store:content_hierarchy
   * @param allCourseProgramDetailsWithCompDF course details with competencies json
   *                                          DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                          courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)
   */
  def allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = allCourseProgramDetailsWithCompDF.filter(col("competenciesJson").isNotNull)
    df = df.withColumn("competencies", from_json(col("competenciesJson"), Schema.courseCompetenciesSchema))

    df = df.select(
      col("courseID"), col("category"), col("courseName"), col("courseStatus"),
      col("courseReviewStatus"), col("courseOrgID"), col("courseOrgName"), col("courseOrgStatus"),
      col("courseDuration"), col("courseResourceCount"),
      explode_outer(col("competencies")).alias("competency")
    )
    df = df.filter(col("competency").isNotNull)
    df = df.withColumn("competencyLevel", expr("TRIM(competency.selectedLevelLevel)"))
    df = df.withColumn("competencyLevel",
      expr("IF(competencyLevel RLIKE '[0-9]+', CAST(REGEXP_EXTRACT(competencyLevel, '[0-9]+', 0) AS INTEGER), 1)"))
    df = df.select(
      col("courseID"), col("category"), col("courseName"), col("courseStatus"),
      col("courseReviewStatus"), col("courseOrgID"), col("courseOrgName"), col("courseOrgStatus"),
      col("courseDuration"), col("courseResourceCount"),
      col("competency.id").alias("competencyID"),
      col("competency.name").alias("competencyName"),
      col("competency.competencyType").alias("competencyType"),
      col("competencyLevel")
    )

    show(df, "allCourseProgramCompetencyDataFrame (courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)")
    df
  }

  /**
   *
   * @param allCourseProgramCompetencyDF DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *                                     courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)
   * @return DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         competencyID, competencyLevel)
   */
  def liveCourseCompetencyDataFrame(allCourseProgramCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = allCourseProgramCompetencyDF.where(expr("courseStatus='Live' AND category='Course'"))
      .select("courseID", "courseName", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration",
        "courseResourceCount", "competencyID", "competencyLevel")

    show(df, "liveCourseCompetencyDataFrame (courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyLevel)")
    df
  }


  /**
   * data frame of course rating summary
   * @return DataFrame(courseID, categoryLower, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
   */
  def courseRatingSummaryDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = cache.load("ratingSummary")
      .where(expr("total_number_of_ratings > 0"))
      .withColumn("ratingAverage", expr("sum_of_total_ratings / total_number_of_ratings"))
      .select(
        col("activityid").alias("courseID"),
        col("activitytype").alias("categoryLower"),
        col("sum_of_total_ratings").alias("ratingSum"),
        col("total_number_of_ratings").alias("ratingCount"),
        col("ratingAverage"),
        col("totalcount1stars").alias("count1Star"),
        col("totalcount2stars").alias("count2Star"),
        col("totalcount3stars").alias("count3Star"),
        col("totalcount4stars").alias("count4Star"),
        col("totalcount5stars").alias("count5Star")
      )
    show(df, "courseRatingSummaryDataFrame before duplicate drop")

    df = df.withColumn("categoryLower", lower(col("categoryLower")))
      .dropDuplicates("courseID", "categoryLower")

    show(df, "courseRatingSummaryDataFrame")
    df
  }

  def orgHierarchyDataframe()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val orgHDF = cache.load("orgHierarchy")
      .select(
        col("mdo_id").alias("userOrgID"),
        col("department").alias("dept_name"),
        col("ministry").alias("ministry_name")
      )
    show(orgHDF, "orgHierarchyDataframe")
    orgHDF
  }

  def userCourseRatingDataframe()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = cache.load("rating")
      .select(
        col("activityid").alias("courseID"),
        col("userid").alias("userID"),
        col("rating").alias("userRating"),
        col("activitytype").alias("cbpType"),
        col("createdon").alias("createdOn")
      )
    show(df, "Rating given by user")
    df
  }

  /**
   * add course rating columns to course detail data-frame
   * @param allCourseProgramDetailsDF course details data frame -
   *                                  DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                  courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   * @param courseRatingDF course rating summary data frame -
   *                       DataFrame(courseID, ratingSum, ratingCount, ratingAverage,
   *                       count1Star, count2Star, count3Star, count4Star, count5Star)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus, courseDuration, courseResourceCount, ratingSum, ratingCount, ratingAverage, count1Star,
   *         count2Star, count3Star, count4Star, count5Star)
   */
  def allCourseProgramDetailsWithRatingDataFrame(allCourseProgramDetailsDF: DataFrame, courseRatingDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = allCourseProgramDetailsDF.withColumn("categoryLower", expr("LOWER(category)"))
      .join(courseRatingDF, Seq("courseID", "categoryLower"), "left")

    show(df)
    df
  }

  /**
   * Batch details for all kind of CBPs
   *
   * @return col(courseID, courseBatchID, courseBatchEnrolmentType, courseBatchName, courseBatchStartDate, courseBatchEndDate,
   *         courseBatchStatus, courseBatchUpdatedDate)
   */
  def courseBatchDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = cache.load("batch")
      .select(
        col("courseid").alias("courseID"),
        col("batchid").alias("batchID"),
        col("name").alias("courseBatchName"),
        col("createdby").alias("courseBatchCreatedBy"),
        col("start_date").alias("courseBatchStartDate"),
        col("end_date").alias("courseBatchEndDate"),
        col("batch_attributes").alias("courseBatchAttrs")
      ).na.fill("{}", Seq("courseBatchAttrs"))
    show(df, "Course Batch Data")
    df
  }


  /**
   * Despite the name this gets all rows from user_enrolments table, only filtering out active=false
   * 'active' column was added to the db to fix the issue of duplicate rows in this table
   *
   * @return DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus)
   */
  def userCourseProgramCompletionDataFrame(extraCols: Seq[String] = Seq(), datesAsLong: Boolean = false)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    val selectCols = Seq("userID", "courseID", "batchID", "courseProgress", "dbCompletionStatus", "courseCompletedTimestamp",
      "courseEnrolledTimestamp", "lastContentAccessTimestamp", "issuedCertificateCount", "firstCompletedOn", "certificateGeneratedOn", "certificateID") ++ extraCols

    var df = cache.load("enrolment")
      .where(expr("active=true"))
      .withColumn("courseCompletedTimestamp", col("completedon"))
      .withColumn("courseEnrolledTimestamp", col("enrolled_date"))
      .withColumn("lastContentAccessTimestamp", col("lastcontentaccesstime"))
      .withColumn("issuedCertificateCount", size(col("issued_certificates")))
      .withColumn("certificateGeneratedOn", when(col("issued_certificates").isNull, "").otherwise( col("issued_certificates")(0).getItem("lastIssuedOn")))
      .withColumn("firstCompletedOn", when(col("issued_certificates").isNull, "").otherwise(when(size(col("issued_certificates")) > 0, col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("lastIssuedOn")).otherwise("")))
      .withColumn("certificateID", when(col("issued_certificates").isNull, "").otherwise( col("issued_certificates")(0).getItem("identifier")))
      .withColumnRenamed("userid", "userID")
      .withColumnRenamed("courseid", "courseID")
      .withColumnRenamed("batchid", "batchID")
      .withColumnRenamed("progress", "courseProgress")
      .withColumnRenamed("status", "dbCompletionStatus")
      .withColumnRenamed("contentstatus", "courseContentStatus")
      .na.fill(0, Seq("courseProgress", "issuedCertificateCount"))
      .na.fill("", Seq("certificateGeneratedOn"))
      .select(selectCols.head, selectCols.tail: _*)

    if (datesAsLong) {
      df = df
        .withColumn("courseCompletedTimestamp", col("courseCompletedTimestamp").cast("long"))
        .withColumn("courseEnrolledTimestamp", col("courseEnrolledTimestamp").cast("long"))
        .withColumn("lastContentAccessTimestamp", col("lastContentAccessTimestamp").cast("long"))
    }

    show(df)
    df
  }

  def leafNodesDataframe(allCourseProgramDF: DataFrame, hierarchyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = hierarchyDF.withColumn("hierarchy", from_json(col("hierarchy"), Schema.makeHierarchySchema()))
    df = df.select(col("hierarchy.leafNodes").alias("liveContents"),
      col("hierarchy.leafNodesCount").alias("liveContentCount"),
      col("identifier"))
    show(df, "leafNodes data")
    df
  }

  def courseStatusUpdateDataFrame(hierarchyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = hierarchyDF.withColumn("hierarchy", from_json(col("hierarchy"), Schema.makeHierarchySchema()))
    df = df
      .select(
        col("hierarchy.lastStatusChangedOn").alias("lastStatusChangedOn"),
        col("identifier").alias("courseID"),
        col("hierarchy.status").alias("courseStatus"))

    val caseExpressionStatus = "CASE WHEN courseStatus == 'Retired' THEN lastStatusChangedOn ELSE '' END"
    df = df.withColumn("ArchivedOn", expr(caseExpressionStatus))
    show(df, "leafNodes data")
    df
  }

  def withCompletionPercentageColumn(df: DataFrame): DataFrame = {
    df
      .withColumn("completionPercentage", expr("CASE WHEN courseResourceCount=0 OR courseProgress=0 OR dbCompletionStatus=0 THEN 0.0 WHEN dbCompletionStatus=2 THEN 100.0 ELSE 100.0 * courseProgress / courseResourceCount END"))
      .withColumn("completionPercentage", expr("CASE WHEN completionPercentage > 100.0 THEN 100.0 WHEN completionPercentage < 0.0 THEN 0.0  ELSE completionPercentage END"))
  }

  /**
   * completionPercentage   completionStatus    IDI status
   * NULL                   not-enrolled        not-started
   * 0.0                    enrolled            not-started
   * 0.0 < % < 10.0         started             enrolled
   * 10.0 <= % < 100.0      in-progress         in-progress
   * 100.0                  completed           completed
   * @param df data frame with completionPercentage column
   * @return df with completionStatus column
   */
  def withOldCompletionStatusColumn(df: DataFrame): DataFrame = {
    val caseExpression = "CASE WHEN ISNULL(completionPercentage) THEN 'not-enrolled' WHEN completionPercentage == 0.0 THEN 'enrolled' WHEN completionPercentage < 10.0 THEN 'started' WHEN completionPercentage < 100.0 THEN 'in-progress' ELSE 'completed' END"
    df.withColumn("completionStatus", expr(caseExpression))
  }

  /**
   * dbCompletionStatus     userCourseCompletionStatus
   * NULL                   not-enrolled
   * 0                      not-started
   * 1                      in-progress
   * 2                      completed
   *
   * @param df data frame with dbCompletionStatus column
   * @return df with userCourseCompletionStatus column
   */
  def withUserCourseCompletionStatusColumn(df: DataFrame): DataFrame = {
    val caseExpression = "CASE WHEN ISNULL(dbCompletionStatus) THEN 'not-enrolled' WHEN dbCompletionStatus == 0 THEN 'not-started' WHEN dbCompletionStatus == 1 THEN 'in-progress' ELSE 'completed' END"
    df.withColumn("userCourseCompletionStatus", expr(caseExpression))
  }

  /**
   * get course completion data with details attached
   * @param userCourseProgramCompletionDF  DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus)
   * @param allCourseProgramDetailsDF course details data frame -
   *                                  DataFrame(courseID, category, courseName, courseStatus,
   *                                  courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   * @param userOrgDF DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus)
   * @return DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp,
   *         courseProgress, dbCompletionStatus, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *         courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, firstName, lastName, maskedEmail, maskedPhone, userStatus,
   *         userOrgID, userOrgName, userOrgStatus, completionPercentage, completionStatus, courseLastPublishedOn)
   */
  def allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF: DataFrame, allCourseProgramDetailsDF: DataFrame, userOrgDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    // userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount
    import spark.implicits._
    var df = userCourseProgramCompletionDF.join(allCourseProgramDetailsDF, Seq("courseID"), "left")
    show(df, "userAllCourseProgramCompletionDataFrame s=0")

    val categoryList = allCourseProgramDetailsDF.select("category").distinct().map(_.getString(0)).filter(_.nonEmpty).collectAsList()
    println("This is the categoryList from allCourseProgramDetailsDF:"+ categoryList)
    df = df.filter(col("category").isInCollection(categoryList))
    println("This is the categoryList :"+ categoryList)
    show(df, "userAllCourseProgramCompletionDataFrame s=1")

    df = df.join(userOrgDF, Seq("userID"), "left")
    df = withCompletionPercentageColumn(df)
    df = withOldCompletionStatusColumn(df)
    df = withUserCourseCompletionStatusColumn(df)

    show(df, "allCourseProgramCompletionWithDetailsDataFrame")

    df
  }

  def calculateCourseProgress(userCourseProgramCompletionDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    // userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount
    var df = withCompletionPercentageColumn(userCourseProgramCompletionDF)
    df = withUserCourseCompletionStatusColumn(df)

    show(df, "allCourseProgramCompletionWithDetailsDataFrame")
    df
  }

  /**
   *
   * @param allCourseProgramCompletionWithDetailsDF DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *         courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *         completionStatus)
   * @return DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *         courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *         completionStatus)
   */
  def liveRetiredCourseCompletionWithDetailsDataFrame(allCourseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = allCourseProgramCompletionWithDetailsDF.where(expr("courseStatus in ('Live', 'Retired') AND category='Course'"))
    show(df, "liveRetiredCourseCompletionWithDetailsDataFrame")
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them from druid
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   */
  def expectedCompetencyDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    val query = """SELECT edata_cb_data_deptId AS orgID, edata_cb_data_wa_id AS workOrderID, edata_cb_data_wa_userId AS userID, edata_cb_data_wa_competency_id AS competencyID, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS expectedCompetencyLevel FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (df == null) return emptySchemaDataFrame(Schema.expectedCompetencySchema)

    df = df.filter(col("competencyID").isNotNull && col("expectedCompetencyLevel").notEqual(0))
      .withColumn("expectedCompetencyLevel", expr("CAST(expectedCompetencyLevel as INTEGER)"))  // Important to cast as integer otherwise a cast will fail later on
      .filter(col("expectedCompetencyLevel").isNotNull && col("expectedCompetencyLevel").notEqual(0))

    show(df)
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them, including live course count
   * @param expectedCompetencyDF expected competency data frame -
   *                             DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName,
   *                               courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyLevel)
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel, liveCourseCount)
   */
  def expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF: DataFrame, liveCourseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    // live course count DF
    val liveCourseCountDF = expectedCompetencyDF.join(liveCourseCompetencyDF, Seq("competencyID"), "left")
      .where(expr("expectedCompetencyLevel <= competencyLevel"))
      .groupBy("orgID", "workOrderID", "userID", "competencyID", "expectedCompetencyLevel")
      .agg(countDistinct("courseID").alias("liveCourseCount"))

    val df = expectedCompetencyDF.join(liveCourseCountDF, Seq("orgID", "workOrderID", "userID", "competencyID", "expectedCompetencyLevel"), "left")
      .na.fill(0, Seq("liveCourseCount"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api
   * @return DataFrame(competencyID, competencyName, competencyStatus)
   */
  def fracCompetencyDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = fracCompetencyDFOption(conf.fracBackendHost).orNull
    if (df == null) return emptySchemaDataFrame(Schema.fracCompetencySchema)

    df = df
      .select(explode_outer(col("data.getAllCompetencies")).alias("competency"))
      .select(
        col("competency.id").alias("competencyID"),
        col("competency.name").alias("competencyName"),
        col("competency.status").alias("competencyStatus")
      )
      .where(expr("LOWER(competencyStatus) = 'verified'"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including live course count
   * @param fracCompetencyDF frac competency data frame -
   *                         DataFrame(competencyID, competencyName, competencyStatus)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration,
   *                               courseResourceCount, competencyID, competencyLevel)
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
   */
  def fracCompetencyWithCourseCountDataFrame(fracCompetencyDF: DataFrame, liveCourseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    // live course count DF
    val liveCourseCountDF = fracCompetencyDF.join(liveCourseCompetencyDF, Seq("competencyID"), "left")
      .filter(col("courseID").isNotNull)
      .groupBy("competencyID", "competencyName", "competencyStatus")
      .agg(countDistinct("courseID").alias("liveCourseCount"))

    val df = fracCompetencyDF.join(liveCourseCountDF, Seq("competencyID", "competencyName", "competencyStatus"), "left")
      .na.fill(0, Seq("liveCourseCount"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including officer count
   * @param fracCompetencyWithCourseCountDF frac competency data frame with live course count
   * @param expectedCompetencyDF expected competency data frame
   * @param declaredCompetencyDF declared  competency data frame
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount, officerCountExpected, officerCountDeclared)
   */
  def fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF: DataFrame, expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    // fracCompetencyWithCourseCountDF = DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
    // expectedCompetencyDF = DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
    // declaredCompetencyDF = DataFrame(userID, competencyID, declaredCompetencyLevel)

    // add expected officer count
    val fcExpectedCountDF = fracCompetencyWithCourseCountDF.join(expectedCompetencyDF, Seq("competencyID"), "leftouter")
      .groupBy("competencyID", "competencyName", "competencyStatus")
      .agg(countDistinct("userID").alias("officerCountExpected"))

    // add declared officer count
    val fcExpectedDeclaredCountDF = fcExpectedCountDF.join(declaredCompetencyDF, Seq("competencyID"), "leftouter")
      .groupBy("competencyID", "competencyName", "competencyStatus", "officerCountExpected")
      .agg(countDistinct("userID").alias("officerCountDeclared"))

    val df = fracCompetencyWithCourseCountDF.join(fcExpectedDeclaredCountDF, Seq("competencyID", "competencyName", "competencyStatus"), "left")

    show(df)
    df
  }

  /**
   * Calculates user's competency gaps
   * @param expectedCompetencyDF expected competency data frame -
   *                             DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   * @param declaredCompetencyDF declared competency data frame -
   *                             DataFrame(userID, competencyID, declaredCompetencyLevel)
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap)
   */
  def competencyGapDataFrame(expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    var df = expectedCompetencyDF.join(declaredCompetencyDF, Seq("competencyID", "userID"), "left")
    df = df.na.fill(0, Seq("declaredCompetencyLevel"))  // if null values created during join fill with 0
    df = df.groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(
        max("expectedCompetencyLevel").alias("expectedCompetencyLevel"),  // in-case of multiple entries, take max
        max("declaredCompetencyLevel").alias("declaredCompetencyLevel")  // in-case of multiple entries, take max
      )
    df = df.withColumn("competencyGap", expr("expectedCompetencyLevel - declaredCompetencyLevel"))

    show(df)
    df
  }

  /**
   * add course data to competency gap data, add user course completion info on top, calculate user competency gap status
   *
   * @param competencyGapDF competency gap data frame -
   *                        DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *                               competencyID, competencyLevel)
   * @param allCourseProgramCompletionWithDetailsDF user course completion data frame -
   *                                                DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *                                                courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *                                                firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *                                                completionStatus)
   *
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, completionPercentage, completionStatus)
   */
  def competencyGapCompletionDataFrame(competencyGapDF: DataFrame, liveCourseCompetencyDF: DataFrame, allCourseProgramCompletionWithDetailsDF: DataFrame): DataFrame = {

    // userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, courseID,
    // courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyLevel
    val cgCourseDF = competencyGapDF.filter("competencyGap > 0")
      .join(liveCourseCompetencyDF, Seq("competencyID"), "leftouter")
      .filter("expectedCompetencyLevel >= competencyLevel")

    // drop duplicate columns before join
    val courseCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDF.drop("courseName", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration", "courseResourceCount")

    // userID, competencyID, orgID, workOrderID, completionPercentage
    val gapCourseUserStatus = cgCourseDF.join(courseCompletionWithDetailsDF, Seq("userID", "courseID"), "left")
      .groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(max(col("completionPercentage")).alias("completionPercentage"))
      .withColumn("completionPercentage", expr("IF(ISNULL(completionPercentage), 0.0, completionPercentage)"))

    var df = competencyGapDF.join(gapCourseUserStatus, Seq("userID", "competencyID", "orgID", "workOrderID"), "left")

    df = withOldCompletionStatusColumn(df)

    show(df)
    df
  }



  /**
   * gets user assessment data from cassandra
   *
   * @return DataFrame(courseID, userID, assessChildID, assessStartTime, assessEndTime, assessUserStatus,
   *         assessTotalQuestions, assessMaxQuestions, assessExpectedDuration, assessVersion
   *         assessMaxRetakeAttempts, assessReadStatus,
   *         assessBatchID, assessPrimaryCategory, assessIsAssessment, assessTimeLimit,
   *         assessResult, assessTotal, assessBlank, assessCorrect, assessIncorrect, assessPass, assessOverallResult,
   *         assessPassPercentage)
   */
  def userAssessmentDataFrame()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): DataFrame = {

    var df = cache.load("userAssessment")
      .select(
        col("assessmentid").alias("assessChildID"),
        col("starttime").alias("assessStartTime"),
        col("endtime").alias("assessEndTime"),
        col("status").alias("assessUserStatus"),
        col("userid").alias("userID"),
        col("assessmentreadresponse"),
        col("submitassessmentresponse"),
        col("submitassessmentrequest")
      )
      .na.fill("{}", Seq("submitassessmentresponse", "submitassessmentrequest"))
      .withColumn("readResponse", from_json(col("assessmentreadresponse"), Schema.assessmentReadResponseSchema))
      .withColumn("submitRequest", from_json(col("submitassessmentrequest"), Schema.submitAssessmentRequestSchema))
      .withColumn("submitResponse", from_json(col("submitassessmentresponse"), Schema.submitAssessmentResponseSchema))
      .withColumn("assessStartTimestamp", col("assessStartTime"))
      .withColumn("assessEndTimestamp", col("assessEndTime"))
      .withColumn("assessStartTime", col("assessStartTime").cast("long"))
      .withColumn("assessEndTime", col("assessEndTime").cast("long"))

    df = df.select(
      col("assessChildID"),
      col("assessStartTime"),
      col("assessEndTime"),
      col("assessUserStatus"),
      col("userID"),
      col("assessStartTimestamp"),
      col("assessEndTimestamp"),

      col("readResponse.totalQuestions").alias("assessTotalQuestions"),
      col("readResponse.maxQuestions").alias("assessMaxQuestions"),
      col("readResponse.expectedDuration").alias("assessExpectedDuration"),
      col("readResponse.version").alias("assessVersion"),
      col("readResponse.maxAssessmentRetakeAttempts").alias("assessMaxRetakeAttempts"),
      col("readResponse.status").alias("assessReadStatus"),
      col("readResponse.primaryCategory").alias("assessPrimaryCategory"),

      col("submitRequest.batchId").alias("assessBatchID"),
      col("submitRequest.courseId").alias("courseID"),
      col("submitRequest.isAssessment").cast(IntegerType).alias("assessIsAssessment"),
      col("submitRequest.timeLimit").alias("assessTimeLimit"),

      col("submitResponse.result").alias("assessResult"),
      col("submitResponse.total").alias("assessTotal"),
      col("submitResponse.blank").alias("assessBlank"),
      col("submitResponse.correct").alias("assessCorrect"),
      col("submitResponse.incorrect").alias("assessIncorrect"),
      col("submitResponse.pass").cast(IntegerType).alias("assessPass"),
      col("submitResponse.overallResult").alias("assessOverallResult"),
      col("submitResponse.passPercentage").alias("assessPassPercentage")
    )

    show(df, "userAssessmentDataFrame")
    df
  }

  /**
   *
   * @param assessWithHierarchyDF
   * @return DataFrame(assessID, assessChildID, assessChildName, assessChildDuration, assessChildPrimaryCategory,
   *         assessChildContentType, assessChildObjectType, assessChildShowTimer, assessChildAllowSkip)
   */
  def assessmentChildrenDataFrame(assessWithHierarchyDF: DataFrame): DataFrame = {
    val df = assessWithHierarchyDF.select(
      col("assessID"), explode_outer(col("children")).alias("ch")
    ).select(
      col("assessID"),
      col("ch.identifier").alias("assessChildID"),
      col("ch.name").alias("assessChildName"),
      col("ch.duration").cast(FloatType).alias("assessChildDuration"),
      col("ch.primaryCategory").alias("assessChildPrimaryCategory"),
      col("ch.contentType").alias("assessChildContentType"),
      col("ch.objectType").alias("assessChildObjectType"),
      col("ch.showTimer").alias("assessChildShowTimer"),
      col("ch.allowSkip").alias("assessChildAllowSkip")
    )

    show(df)
    df
  }

  /**
   *
   * @param userAssessmentDF
   * @param assessChildrenDF
   * @return DataFrame(courseID, userID, assessChildID, assessStartTime, assessEndTime, assessUserStatus,
   *         assessTotalQuestions, assessMaxQuestions, assessExpectedDuration, assessVersion
   *         assessMaxRetakeAttempts, assessReadStatus,
   *         assessBatchID, assessPrimaryCategory, assessIsAssessment, assessTimeLimit,
   *         assessResult, assessTotal, assessBlank, assessCorrect, assessIncorrect, assessPass, assessOverallResult,
   *         assessPassPercentage,
   *
   *         assessID, assessChildName, assessChildDuration, assessChildPrimaryCategory,
   *         assessChildContentType, assessChildObjectType, assessChildShowTimer, assessChildAllowSkip)
   */
  def userAssessmentChildrenDataFrame(userAssessmentDF: DataFrame, assessChildrenDF: DataFrame): DataFrame = {
    val df = userAssessmentDF.join(assessChildrenDF, Seq("assessChildID"), "inner")

    show(df)
    df
  }

  /**
   * gets user assessment data from cassandra
   *
   * @return DataFrame(courseID, userID, assessChildID, assessStartTime, assessEndTime, assessUserStatus,
   *         assessTotalQuestions, assessMaxQuestions, assessExpectedDuration, assessVersion,
   *         assessMaxRetakeAttempts, assessReadStatus,
   *         assessBatchID, assessPrimaryCategory, assessIsAssessment, assessTimeLimit,
   *         assessResult, assessTotal, assessBlank, assessCorrect, assessIncorrect, assessPass, assessOverallResult,
   *         assessPassPercentage,
   *
   *         assessID, assessChildName, assessChildDuration, assessChildPrimaryCategory,
   *         assessChildContentType, assessChildObjectType, assessChildShowTimer, assessChildAllowSkip
   *
   *         assessCategory, assessName, assessStatus, assessReviewStatus, assessOrgID,
   *         assessOrgName, assessOrgStatus, assessDuration, assessChildCount,
   *         assessPublishType, assessIsExternal, assessContentType, assessObjectType, assessUserConsent,
   *         assessVisibility, assessCreatedOn, assessLastUpdatedOn, assessLastPublishedOn, assessLastSubmittedOn,
   *
   *         category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus, courseDuration, courseResourceCount, ratingSum, ratingCount, ratingAverage,
   *
   *         firstName, lastName, maskedEmail, userStatus, userCreatedTimestamp, userUpdatedTimestamp, userOrgID,
   *         userOrgName, userOrgStatus)
   */
  def userAssessmentChildrenDetailsDataFrame(userAssessChildrenDF: DataFrame, assessWithDetailsDF: DataFrame, allCourseProgramDetailsWithRatingDF: DataFrame, userOrgDF: DataFrame): DataFrame = {

    val courseDF = allCourseProgramDetailsWithRatingDF
      .drop("count1Star", "count2Star", "count3Star", "count4Star", "count5Star")
    val df = userAssessChildrenDF
      .join(assessWithDetailsDF, Seq("assessID"), "left")
      .join(courseDF, Seq("courseID"), "left")
      .join(userOrgDF, Seq("userID"), "left")

    show(df, "userAssessmentDetailsDataFrame")
    df
  }

  def orgDesignationsDF(userOrgDF: DataFrame)(implicit sparkSession: SparkSession, conf: DashboardConfig): DataFrame = {
    val userDesignationListDF = userOrgDF.withColumn("designation", col("professionalDetails.designation"))
    val orgDesignationListDF = userDesignationListDF.groupBy("userOrgID").agg(concat_ws(",", collect_set("designation")).alias("org_designations"))
    orgDesignationListDF
  }

  def mdoIDsDF(mdoID: String)(implicit spark: SparkSession, sc: SparkContext): DataFrame = {
    val mdoIDs = mdoID.split(",").map(_.toString).distinct
    val rdd = sc.parallelize(mdoIDs)

    val rowRDD: RDD[Row] = rdd.map(t => Row(t))

    val schema = new StructType()
      .add(StructField("orgID", StringType, nullable = false))
    val df = spark.createDataFrame(rowRDD, schema)
    df
  }

  /**
   * Reading existing weekly claps data
   */
  def learnerStatsDataFrame()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): DataFrame = {
    val df = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraLearnerStatsTable)
    show(df, "Learner stats data")
    df
  }

  /**
   * Reading karma points details
   */
  def userKarmaPointsSummaryDataFrame()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): DataFrame = {
    val df = cache.load("userKarmaPointsSummary")
    show(df, "Karma Points Summary data")
    df
  }

  /**
   * Reading user_karma_points data
   */
  def userKarmaPointsDataFrame()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): DataFrame = {
    val df = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)
    show(df, "Karma Points data")
    df
  }

  /* telemetry data frames */

  def loggedInMobileUserDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = """SELECT DISTINCT(actor_id) AS userID FROM \"telemetry-events-syncts\" WHERE eid='IMPRESSION' AND actor_type='User' AND context_pdata_pid IN ('karmayogi-mobile-android', 'karmayogi-mobile-ios')"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost, limit = 1000000).orNull
    if (df == null) return emptySchemaDataFrame(Schema.loggedInMobileUserSchema)
    df = df.withColumn("userLoginFromMobile", lit(true))

    show(df, "loggedInMobileUserDataFrame")
    df
  }

  def loggedInWebUserDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = """SELECT DISTINCT(actor_id) AS userID FROM \"telemetry-events-syncts\" WHERE eid='IMPRESSION' AND actor_type='User' AND context_pdata_pid IN ('sunbird-cb-orgportal', 'sunbird-cb-portal')"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost, limit = 1000000).orNull
    if (df == null) return emptySchemaDataFrame(Schema.loggedInWebUserSchema)
    df = df.withColumn("userLoginFromWeb", lit(true))

    show(df, "loggedInWebUserDataFrame")
    df
  }

  def actualTimeSpentLearningDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = """SELECT uid AS userID, SUM(total_time_spent) AS userActualTimeSpentLearning FROM \"summary-events\" WHERE dimensions_type<>'app' AND object_type IN ('Learning Resource', 'Practice Question Set', 'Course Assessment') GROUP BY 1"""
    val df = druidDFOption(query, conf.sparkDruidRouterHost, limit = 1000000).orNull
    if (df == null) return emptySchemaDataFrame(Schema.userActualTimeSpentLearningSchema)

    show(df, "actualTimeSpentLearningDataFrame")
    df
  }

  /**
   * Get user engagement data - timespent and number of sessions from druid summary-events for the current week (Mon - sun)
   */
  def usersPlatformEngagementDataframe(weekStart: String, weekEnd: String)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = raw"""SELECT uid AS userid, SUM(total_time_spent) / 60.0 AS platformEngagementTime, COUNT(*) AS sessionCount FROM \"summary-events\" WHERE dimensions_type='app' AND __time >= TIMESTAMP '${weekStart}' AND __time <= TIMESTAMP '${weekEnd}' AND uid IS NOT NULL GROUP BY 1"""
    val df = druidDFOption(query, conf.sparkDruidRouterHost, limit = 1000000).orNull
    if (df == null) return emptySchemaDataFrame(Schema.usersPlatformEngagementSchema)
    show(df, "usersPlatformEngagementDataframe")
    df
  }

  /**
   * gets the user_id and survey_submitted_time from cassandra
   * gets the user_ids who have submitted the survey in last 3 months
   */
  def npsTriggerC1DataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = """SELECT userID as userid FROM \"nps-users-data\" where  __time >= CURRENT_TIMESTAMP - INTERVAL '3' MONTH"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost, limit = 1000000).orNull
    if(df == null) return emptySchemaDataFrame(Schema.npsUserIds)
    df = df.na.drop(Seq("userid"))
    df
  }

  def npsTriggerC2DataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = """(SELECT DISTINCT(userID) as userid FROM \"dashboards-user-course-program-progress\" WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\") AND courseCompletedTimestamp >= TIMESTAMP_TO_MILLIS(__time + INTERVAL '5:30' HOUR TO MINUTE - INTERVAL '3' MONTH) / 1000.0 AND category IN ('Course','Program') AND courseStatus IN ('Live', 'Retired') AND dbCompletionStatus = 2) UNION ALL (SELECT uid as userid FROM (SELECT SUM(total_time_spent) AS totalTime, uid FROM \"summary-events\" WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '3' MONTH AND dimensions_type='app' GROUP BY 2) WHERE totalTime >= 7200)"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost, limit = 1000000).orNull
    if (df == null) return emptySchemaDataFrame(Schema.npsUserIds)
    df = df.dropDuplicates("userid")
    df
  }

  def npsTriggerC3DataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = mongodbTableAsDataFrame(conf.mongoDatabase, conf.mongoDBCollection)
    if (df == null) return emptySchemaDataFrame(Schema.npsUserIds)
    df = df.na.drop(Seq("userid"))
    df
  }

  def userFeedFromCassandraDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = cassandraTableAsDataFrame(conf.cassandraUserFeedKeyspace, conf.cassandraUserFeedTable)
      .select(col("userid").alias("userid"))
      .where(col("category") === "NPS")
    if(df == null) return emptySchemaDataFrame(Schema.npsUserIds)
    df = df.na.drop(Seq("userid"))
    df
  }

  def acbpDetailsDF()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = cache.load("acbp")
      .select(
        col("id").alias("acbpID"),
        col("orgid").alias("userOrgID"),
        col("draftdata"),
        col("status").alias("acbpStatus"),
        col("createdby").alias("acbpCreatedBy"),
        col("name").alias("cbPlanName"),
        col("assignmenttype").alias("assignmentType"),
        col("assignmenttypeinfo").alias("assignmentTypeInfo"),
        col("enddate").alias("completionDueDate"),
        col("publishedat").alias("allocatedOn"),
        col("contentlist").alias("acbpCourseIDList")
      ).na.fill("", Seq("cbPlanName"))
    show(df, "acbpDetails")

    var draftCBPData = df.filter(col("acbpStatus") === "DRAFT" && col("draftdata").isNotNull)
    draftCBPData = draftCBPData.select(col("acbpID"),col("userOrgID"),col("draftdata"),col("acbpStatus"),col("acbpCreatedBy"))
      .withColumn("draftData", from_json(col("draftdata"), Schema.cbplanDraftDataSchema))
      .withColumn("cbPlanName", col("draftData.name"))
      .withColumn("assignmentType", col("draftData.assignmentType"))
      .withColumn("assignmentTypeInfo", col("draftData.assignmentTypeInfo"))
      .withColumn("completionDueDate", col("draftData.endDate"))
      .withColumn("allocatedOn", lit("not published"))
      .withColumn("acbpCourseIDList", col("draftData.contentList"))
      .drop("draftData")
    // get the live and retire ACBP data
    val nonDraftCBPData = df.filter(col("acbpStatus") =!= "DRAFT")
    // union draft and non-draft
    val result = nonDraftCBPData.union(draftCBPData)
    show(result, "allACBPDetails")
    result
  }

  /* report generation stuff

  this is here instead of in DashboardUtil to avoid the conflict b/w
  org.sunbird.cloud.storage.factory.StorageConfig and
  org.ekstep.analytics.framework.StorageConfig */

  def syncReports(reportTempPath: String, reportPath: String)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    println(s"REPORT: Syncing reports from ${reportTempPath} to ${conf.store}://${conf.container}/${reportPath} ...")
    val storageService = StorageUtil.getStorageService(conf)
    // upload files to - {store}://{container}/{reportPath}/
    val storageConfig = new StorageConfig(conf.store, conf.container, reportTempPath)
    storageService.upload(storageConfig.container, reportTempPath, s"${reportPath}/", Some(true), Some(0), Some(3), None)
    storageService.closeContext()
    println(s"REPORT: Finished syncing reports from ${reportTempPath} to ${conf.store}://${conf.container}/${reportPath}")
  }

  def generateAndSyncReports(df: DataFrame, partitionKey: String, reportPath: String, fileName: String)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val reportTempPath = s"${conf.localReportDir}/${reportPath}"
    generateReport(df, reportPath, partitionKey, fileName)
    syncReports(reportTempPath, reportPath)
  }

  def learnerLeaderBoardDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = cache.load("learnerLeaderBoard")
    show(df, "learnerLeaderBoard")
    df
  }

  def getSolutionIdsAsDF(solutionIds: String)(implicit spark: SparkSession, sc: SparkContext): DataFrame = {
    val mdoIDs = solutionIds.split(",").map(_.toString).distinct
    val rdd = sc.parallelize(mdoIDs)
    val rowRDD: RDD[Row] = rdd.map(t => Row(t))
    val schema = new StructType()
      .add(StructField("solutionIds", StringType, nullable = false))
    val df = spark.createDataFrame(rowRDD, schema)
    df
  }

  def getSolutionIdData(columns: String, dataSource: String, solutionId: String)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = raw"""SELECT $columns FROM  \"$dataSource\" WHERE solutionId='$solutionId'"""
    var df = druidDFOption(query, conf.mlSparkDruidRouterHost, limit = 1000000).orNull
    if (df == null) return emptySchemaDataFrame(Schema.solutionIdDataSchema)
    if (df.columns.contains("evidences")) {
      val baseUrl = conf.baseUrlForEvidences
      val addBaseUrl = udf((evidences: String) => {
        if (evidences != null && evidences.trim.nonEmpty) {
          evidences.split(", ")
            .map(url => s"$baseUrl$url")
            .mkString(",")
        } else {
          evidences
        }
      })
      df = df.withColumn("evidences", addBaseUrl(col("evidences")))
    }
    df
  }

  def processProfileData(originalDf: DataFrame, profileSchema: StructType, requiredCsvColumns: List[Column])(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val parsedDf = originalDf.withColumn("parsedProfile", from_json(col("userProfile"), profileSchema))
    val finalDF = parsedDf.select(requiredCsvColumns: _*)
    finalDF
  }

  def validateColumns(df: DataFrame, columns: Seq[String]): Boolean = {
    val dfColumnsSet = df.columns.map(_.trim).toSet
    val columnsSet = columns.map(_.trim).toSet
    dfColumnsSet == columnsSet
  }

  def loadAllUniqueSolutionIds(dataSource: String)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = raw"""SELECT DISTINCT solutionId AS solutionIds, solutionName FROM \"$dataSource\" """
    var df = druidDFOption(query, conf.mlSparkDruidRouterHost, limit = 1000000).orNull
    if (df == null) return emptySchemaDataFrame(Schema.uniqueSolutionIdsDataSchema)
    df = df.dropDuplicates("solutionIds")
    df
  }

  def getSolutionsEndDate(solutionIdsDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val completeUrl = s"mongodb://${conf.mlSparkMongoConnectionHost}:27017"
    val df = mongodbSolutionsTableAsDataFrame(completeUrl, conf.mlMongoDatabase, conf.surveyCollection, solutionIdsDF)
    if (df == null) return emptySchemaDataFrame(Schema.solutionsEndDateDataSchema)
    df
  }

  def getReportConfig(filter: String)(implicit spark: SparkSession, conf: DashboardConfig): String = {
    val completeUrl = s"mongodb://${conf.mlSparkMongoConnectionHost}:27017"
    val reportConfig = mongodbReportConfigAsString(completeUrl, conf.mlMongoDatabase, conf.reportConfigCollection, filter)
    reportConfig
  }

  def zipAndSyncReports(completePath: String, reportPath: String)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val folder = new File(completePath)
    val zipFilePath = completePath + ".zip"
    /** Delete the existing .zip file if it exists */
    val reportName = completePath.split("/").last
    val existingZipFile = new File(completePath + s"/$reportName.zip")
    if (existingZipFile.exists()) existingZipFile.delete()
    /** Delete .crc files */
    val crcFiles = folder.listFiles.filter(_.getName.endsWith(".crc"))
    crcFiles.foreach(_.delete())
    /** Zip the folder */

    val zipFile = new ZipFile(zipFilePath)
    val parameters = new ZipParameters()
    parameters.setCompressionMethod(CompressionMethod.DEFLATE)
    parameters.setCompressionLevel(CompressionLevel.NORMAL)

    zipFile.addFolder(folder, parameters)
    /** Delete all files inside parent directory */
    if (folder.isDirectory) FileUtils.cleanDirectory(folder)
    /** Move the zip file inside the parent directory */
    val zipFileName = new File(zipFilePath).getName()
    val destinationFolderPath = completePath
    val destinationZipFilePath = destinationFolderPath + File.separator + zipFileName
    new File(zipFilePath).renameTo(new File(destinationZipFilePath))
    /** Upload file to blob storage */
    syncReports(completePath, reportPath)
  }

  def getObservationStatusCompletedData(solutionDf: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val modifiedSolutionDf = solutionDf
      .withColumn("Status of Submission", lit(null).cast(StringType))
      .withColumn("Submission Date", lit(null).cast(StringType))
    val query = """SELECT completedAt, observationSubmissionId FROM \"sl-observation-status-completed\" """
    val statusCompletedQueryDf = druidDFOption(query, conf.mlSparkDruidRouterHost, limit = 1000000).orNull
    if (statusCompletedQueryDf == null) return emptySchemaDataFrame(Schema.observationStatusCompletedDataSchema)
    statusCompletedQueryDf.dropDuplicates()

    val statusCompletedJoinDf = modifiedSolutionDf.join(statusCompletedQueryDf, modifiedSolutionDf("Observation Submission Id") === statusCompletedQueryDf("observationSubmissionId"), "left")
    val statusCompletedFinalDf = statusCompletedJoinDf
      .withColumn("Status of Submission", when(col("observationSubmissionId").isNotNull, lit("Completed")).otherwise(col("Status of Submission")))
      .withColumn("Submission Date", when(col("observationSubmissionId").isNotNull, col("completedAt")).otherwise(col("Submission Date")))
      .drop("completedAt", "observationSubmissionId")
    statusCompletedFinalDf
  }

  def getObservationStatusInProgressData(solutionDf: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val query = """SELECT observationSubmissionId FROM \"sl-observation-status-inprogress\" """
    val statusInProgressQueryDf = druidDFOption(query, conf.mlSparkDruidRouterHost, limit = 1000000).orNull
    if (statusInProgressQueryDf == null) return emptySchemaDataFrame(Schema.observationStatusInProgressDataSchema)
    statusInProgressQueryDf.dropDuplicates()

    val statusInProgressJoinDf = solutionDf.join(statusInProgressQueryDf, solutionDf("Observation Submission Id") === statusInProgressQueryDf("observationSubmissionId"), "left")
    val statusInProgressFinalDf = statusInProgressJoinDf
      .withColumn("Status of Submission", when(col("observationSubmissionId").isNotNull, lit("In Progress")).otherwise(col("Status of Submission")))
      .drop("observationSubmissionId")
    statusInProgressFinalDf
  }

}
