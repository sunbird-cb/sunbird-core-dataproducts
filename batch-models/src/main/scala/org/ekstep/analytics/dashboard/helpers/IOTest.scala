package org.ekstep.analytics.dashboard.helpers

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.FrameworkContext

object IOTest extends Serializable {

  val rootPath = "/home/analytics/io-test"

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("IOTest")
        .config("spark.master", "local[*]")
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val fc: FrameworkContext = new FrameworkContext()
    sc.setLogLevel("WARN")
    val res = time(test(args))
    Console.println("Time taken to execute script", res._1)
    spark.stop()
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

  def test(args: Array[String])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    val executionTime = System.currentTimeMillis()
    processData(executionTime, args)
  }

  def processData(startTimestamp: Long, args: Array[String])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    println("Spark Config:")
    println(spark.conf.getAll)

    println(s"startTimestamp=${startTimestamp}")

    val writeStats = args.map(filename => testWrite(filename))
    val ioStats  = writeStats.map(writeInfo => writeInfo ++ testRead(writeInfo))
    ioStats.foreach(s => println(s))
  }

  def testWrite(filename: String)(implicit spark: SparkSession): Map[String, Any] = {
    // following 2 lines should force spark to read all of the json data in memory and avoid any lazy loading
    val df = spark.read.json(s"${rootPath}/${filename}").coalesce(1).persist(StorageLevel.MEMORY_ONLY)
    val count = df.rdd.count()

    // test parquet write
    val parquetOutPath = s"${rootPath}/out/parquet/${filename}"
    val parquetStart = System.currentTimeMillis()
    df.write.mode(SaveMode.Overwrite).parquet(parquetOutPath)
    val parquetDurationMills = System.currentTimeMillis() - parquetStart

    // test avro write
    val avroOutPath = s"${rootPath}/out/avro/${filename}.avro"
    val avroStart = System.currentTimeMillis()
    df.write.mode(SaveMode.Overwrite).format("avro").save(avroOutPath)
    val avroDurationMills = System.currentTimeMillis() - avroStart

    // test csv write
    val csvOutPath = s"${rootPath}/out/csv/${filename}"
    val csvStart = System.currentTimeMillis()
    df.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(csvOutPath)
    val csvDurationMills = System.currentTimeMillis() - csvStart

    // free data frame memory
    df.unpersist(blocking = true)

    Map(
      "filename" -> filename,
      "count"-> count,
      "parquetOutPath" -> parquetOutPath,
      "avroOutPath" -> avroOutPath,
      "csvOutPath" -> csvOutPath,
      "parquetWriteDurationMills" -> parquetDurationMills,
      "avroWriteDurationMills" -> avroDurationMills,
      "csvWriteDurationMills" -> csvDurationMills
    )
  }

  def testRead(writeInfo: Map[String, Any])(implicit spark: SparkSession): Map[String, Any] = {

    // read parquet
    val parquetStart = System.currentTimeMillis()
    val parquetDF = spark.read.parquet(writeInfo.get("parquetOutPath").asInstanceOf[String]).persist(StorageLevel.MEMORY_ONLY)
    val parquetOutCount = parquetDF.rdd.count()
    val parquetDurationMills = System.currentTimeMillis() - parquetStart
    parquetDF.unpersist(blocking = true)

    // read avro
    val avroStart = System.currentTimeMillis()
    val avroDF = spark.read.format("avro").load(writeInfo.get("avroOutPath").asInstanceOf[String]).persist(StorageLevel.MEMORY_ONLY)
    val avroOutCount = avroDF.rdd.count()
    val avroDurationMills = System.currentTimeMillis() - avroStart
    avroDF.unpersist(blocking = true)

    // read csv
    val csvStart = System.currentTimeMillis()
    val csvDF = spark.read.format("csv").option("header", "true").load(writeInfo.get("csvOutPath").asInstanceOf[String]).persist(StorageLevel.MEMORY_ONLY)
    val csvOutCount = csvDF.rdd.count()
    val csvDurationMills = System.currentTimeMillis() - csvStart
    csvDF.unpersist(blocking = true)

    Map(
      "parquetOutCount" -> parquetOutCount,
      "avroOutCount" -> avroOutCount,
      "csvOutCount" -> csvOutCount,
      "parquetReadDurationMills" -> parquetDurationMills,
      "avroReadDurationMills" -> avroDurationMills,
      "csvWReadDurationMills" -> csvDurationMills
    )
  }

}
