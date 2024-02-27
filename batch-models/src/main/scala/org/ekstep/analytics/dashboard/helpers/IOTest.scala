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
    import spark.implicits._
    println("Spark Config:")
    println(spark.conf.getAll)

    println(s"startTimestamp=${startTimestamp}")
    implicit val testCsv: Boolean = false

    val writeStats = args.flatMap(filename => testWrite(filename))
    val readStats = writeStats.map(writeInfo => testRead(writeInfo))
    val ioStats  = readStats ++ writeStats

    val statsDF = ioStats.toList.map(s => (
      getFromMap[String](s, "filename"),
      getFromMap[Long](s, "count"),
      getFromMap[Double](s, "jsonLoadDuration"),
      getFromMap[String](s, "compression"),
      getFromMap[String](s, "operation"),
      getFromMap[Double](s, "parquetDuration"),
      getFromMap[Double](s, "avroDuration"),
      getFromMap[Double](s, "csvDuration")
    )).toDF("filename", "rows", "json load time", "compression", "operation", "parquet time", "avro time", "csv time")

    println("Writing stats CSV...")
    statsDF.coalesce(1)
      .write.mode(SaveMode.Overwrite).option("header", "true").format("csv").save(s"${rootPath}/out/stats")

  }

  def testWrite(filename: String)(implicit spark: SparkSession, testCsv: Boolean): Seq[Map[String, Any]] = {
    // following 2 lines should force spark to read all of the json data in memory and avoid any lazy loading
    println(s"Loading ${filename}")
    val loadStart = System.currentTimeMillis()
    val df = spark.read.json(s"${rootPath}/${filename}").coalesce(1).persist(StorageLevel.MEMORY_ONLY)
    val count = df.rdd.count()
    val loadDuration = (System.currentTimeMillis() - loadStart) / 1000.0
    df.printSchema()
    df.show()
    println(s"Loading ${filename}: DONE in ${loadDuration}s rows=${count}")

    val res = Seq("uncompressed", "snappy").map(compression => {
      // test parquet write
      println(s"Testing parquet write:")
      val parquetOutPath = s"${rootPath}/out/parquet/${filename}-${compression}"
      val parquetStart = System.currentTimeMillis()
      df.write.mode(SaveMode.Overwrite).option("compression", compression).parquet(parquetOutPath)
      val parquetDuration = (System.currentTimeMillis() - parquetStart) / 1000.0
      println(s"Testing parquet write: DONE in ${parquetDuration}s")

      // test avro write
      println(s"Testing avro write:")
      val avroOutPath = s"${rootPath}/out/avro/${filename}-${compression}"
      val avroStart = System.currentTimeMillis()
      df.write.mode(SaveMode.Overwrite).option("compression", compression).format("avro").save(avroOutPath)
      val avroDuration = (System.currentTimeMillis() - avroStart) / 1000.0
      println(s"Testing avro write: DONE in ${avroDuration}s")

      // test csv write
      var csvOutPath = "N/A"
      var csvDuration = 0.0
      if (testCsv) {
        println(s"Testing csv write:")
        csvOutPath = s"${rootPath}/out/csv/${filename}"
        val csvStart = System.currentTimeMillis()
        df.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(csvOutPath)
        csvDuration = (System.currentTimeMillis() - csvStart) / 1000.0
        println(s"Testing csv write: DONE in ${csvDuration}s")
      }

      Map(
        "filename" -> filename,
        "count"-> count,
        "compression" -> compression,
        "operation" -> "write",
        "parquetOutPath" -> parquetOutPath,
        "avroOutPath" -> avroOutPath,
        "csvOutPath" -> csvOutPath,
        "jsonLoadDuration" -> loadDuration,
        "parquetDuration" -> parquetDuration,
        "avroDuration" -> avroDuration,
        "csvDuration" -> csvDuration
      )
    })

    println(s"Freeing up memory...")
    // free data frame memory
    df.unpersist(blocking = true)
    println(s"Freeing up memory: DONE")

    res
  }

  def getFromMap[T](map: Map[String, Any], key: String): T = {
    map.getOrElse(key, null).asInstanceOf[T]
  }

  def testRead(writeInfo: Map[String, Any])(implicit spark: SparkSession, testCsv: Boolean): Map[String, Any] = {
    val filename = getFromMap[String](writeInfo, "filename")
    val count = getFromMap[Long](writeInfo, "count")
    val compression = getFromMap[String](writeInfo, "compression")
    val jsonLoadDuration = getFromMap[Double](writeInfo, "jsonLoadDuration")

    // read parquet
    println(s"Testing parquet read:")
    val parquetStart = System.currentTimeMillis()
    val parquetDF = spark.read.parquet(getFromMap[String](writeInfo, "parquetOutPath")).persist(StorageLevel.MEMORY_ONLY)
    val parquetOutCount = parquetDF.rdd.count()
    val parquetDuration = (System.currentTimeMillis() - parquetStart) / 1000.0
    assert(parquetOutCount == count, "should match original count")
    println(s"Testing parquet read: DONE in ${parquetDuration}s rows=${parquetOutCount}")
    parquetDF.printSchema()
    parquetDF.show()
    parquetDF.unpersist(blocking = true)

    // read avro
    println(s"Testing avro read:")
    val avroStart = System.currentTimeMillis()
    val avroDF = spark.read.format("avro").load(getFromMap[String](writeInfo, "avroOutPath")).persist(StorageLevel.MEMORY_ONLY)
    val avroOutCount = avroDF.rdd.count()
    val avroDuration = (System.currentTimeMillis() - avroStart) / 1000.0
    assert(avroOutCount == count, "should match original count")
    println(s"Testing avro read: DONE in ${avroDuration}s rows=${avroOutCount}")
    avroDF.printSchema()
    avroDF.show()
    avroDF.unpersist(blocking = true)

    // read csv
    var csvOutCount = 0L
    var csvDuration = 0.0
    if (testCsv) {
      println(s"Testing csv read:")
      val csvStart = System.currentTimeMillis()
      val csvDF = spark.read.format("csv").option("header", "true").load(getFromMap[String](writeInfo, "csvOutPath")).persist(StorageLevel.MEMORY_ONLY)
      csvOutCount = csvDF.rdd.count()
      csvDuration = (System.currentTimeMillis() - csvStart) / 1000.0
      assert(csvOutCount == count, "should match original count")
      println(s"Testing csv read: DONE in ${csvDuration}s rows=${csvOutCount}")
      csvDF.printSchema()
      csvDF.show()
      csvDF.unpersist(blocking = true)
    }

    Map(
      "filename" -> filename,
      "count"-> count,
      "compression" -> compression,
      "operation" -> "read",
      "jsonLoadDuration" -> jsonLoadDuration,
      "parquetDuration" -> parquetDuration,
      "avroDuration" -> avroDuration,
      "csvDuration" -> csvDuration
    )
  }

}
