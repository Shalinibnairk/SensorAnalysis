package com.sensor.utilities

import java.io.{FileNotFoundException, FileReader, IOException}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object utilityfunctions {

  def createSession(): SparkSession = {

    //Creating spark Session
    val spark = new SparkSession.Builder().master("local[*]").appName("SensorData").getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    return spark
  }

  def validatePath(path: String): Boolean = {
    // Have to validate the path and throw exception in case of not found

    try {
      val i = new FileReader(path)
    } catch {
      case ex: FileNotFoundException => {
        println("File not found Exception")
        return false
      }
      case ex: IOException => {
        println("Input /Output Exception")
        return false
      }
    }
    return true
  }


  def sparkfilereader(spark: SparkSession, path: String): DataFrame = {

    // Spark reader
    val customschema = StructType(List(
      StructField("Sensor", StringType, true),
      StructField("Mnemonic", StringType, true),
      StructField("data", IntegerType, true),
      StructField("timestamp", LongType, true))
    )

    var load_tb = spark.emptyDataFrame

    try {
      load_tb = spark.read.format("csv")
        .schema(customschema)
        .option("sep", ",")
        .option("inferSchema", "false")
        .option("header", "true").load(path)

      load_tb.printSchema()
    }
    catch {
      case ex: Exception => {
        println(ex.getStackTrace)
      }
    }

    return load_tb

  }
}
