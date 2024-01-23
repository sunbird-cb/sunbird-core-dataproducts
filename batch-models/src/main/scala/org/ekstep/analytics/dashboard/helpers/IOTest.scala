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
    implicit val testCsv: Boolean = true

    val writeStats = args.map(filename => testWrite(filename))
    val ioStats  = writeStats.map(writeInfo => writeInfo ++ testRead(writeInfo))
    ioStats.foreach(s => println(s))
  }

  def testWrite(filename: String)(implicit spark: SparkSession, testCsv: Boolean): Map[String, Any] = {
    // following 2 lines should force spark to read all of the json data in memory and avoid any lazy loading
    println(s"Loading ${filename}")
    val df = spark.read.json(s"${rootPath}/${filename}").coalesce(1).persist(StorageLevel.MEMORY_ONLY)
    val count = df.rdd.count()
    df.printSchema()
    df.show()
    println(s"Loading ${filename}: DONE rows=${count}")

    // test parquet write
    println(s"Testing parquet write:")
    val parquetOutPath = s"${rootPath}/out/parquet/${filename}"
    val parquetStart = System.currentTimeMillis()
    df.write.mode(SaveMode.Overwrite).parquet(parquetOutPath)
    val parquetDurationMills = System.currentTimeMillis() - parquetStart
    println(s"Testing parquet write: DONE in ${parquetDurationMills / 1000}s")

    // test avro write
    println(s"Testing avro write:")
    val avroOutPath = s"${rootPath}/out/avro/${filename}.avro"
    val avroStart = System.currentTimeMillis()
    df.write.mode(SaveMode.Overwrite).format("avro").save(avroOutPath)
    val avroDurationMills = System.currentTimeMillis() - avroStart
    println(s"Testing avro write: DONE in ${avroDurationMills / 1000}s")

    // test csv write
    var csvOutPath = "N/A"
    var csvDurationMills = 0L
    if (testCsv) {
      println(s"Testing csv write:")
      csvOutPath = s"${rootPath}/out/csv/${filename}"
      val csvStart = System.currentTimeMillis()
      df.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(csvOutPath)
      csvDurationMills = System.currentTimeMillis() - csvStart
      println(s"Testing csv write: DONE in ${csvDurationMills / 1000}s")
    }

    println(s"Freeing up memory...")
    // free data frame memory
    df.unpersist(blocking = true)
    println(s"Freeing up memory: DONE")

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

  def getFromMap[T](map: Map[String, Any], key: String): T = {
    map.getOrElse(key, "").asInstanceOf[T]
  }

  def testRead(writeInfo: Map[String, Any])(implicit spark: SparkSession, testCsv: Boolean): Map[String, Any] = {

    // read parquet
    println(s"Testing parquet read:")
    val parquetStart = System.currentTimeMillis()
    val parquetDF = spark.read.parquet(getFromMap[String](writeInfo, "parquetOutPath")).persist(StorageLevel.MEMORY_ONLY)
    val parquetOutCount = parquetDF.rdd.count()
    val parquetDurationMills = System.currentTimeMillis() - parquetStart
    println(s"Testing parquet read: DONE in ${parquetDurationMills / 1000}s rows=${parquetOutCount}")
    parquetDF.printSchema()
    parquetDF.show()
    parquetDF.unpersist(blocking = true)

    // read avro
    println(s"Testing avro read:")
    val avroStart = System.currentTimeMillis()
    val avroDF = spark.read.format("avro").load(getFromMap[String](writeInfo, "avroOutPath")).persist(StorageLevel.MEMORY_ONLY)
    val avroOutCount = avroDF.rdd.count()
    val avroDurationMills = System.currentTimeMillis() - avroStart
    println(s"Testing avro read: DONE in ${avroDurationMills / 1000}s rows=${avroOutCount}")
    avroDF.printSchema()
    avroDF.show()
    avroDF.unpersist(blocking = true)

    // read csv
    var csvOutCount = 0
    var csvDurationMills = 0L
    if (testCsv) {
      println(s"Testing csv read:")
      val csvStart = System.currentTimeMillis()
      val csvDF = spark.read.format("csv").option("header", "true").load(getFromMap[String](writeInfo, "csvOutPath")).persist(StorageLevel.MEMORY_ONLY)
      csvOutCount = csvDF.rdd.count()
      csvDurationMills = System.currentTimeMillis() - csvStart
      println(s"Testing csv read: DONE in ${csvDurationMills / 1000}s rows=${csvOutCount}")
      csvDF.printSchema()
      csvDF.show()
      csvDF.unpersist(blocking = true)
    }

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
