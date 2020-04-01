package com.sensor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object Triggering {

  def identifyTrigger(load_tb:DataFrame):DataFrame={

    // Creating window for lag operation
    val window = Window.orderBy("Sensor")

    // Created new 2 columns for laf Sensor data and lag time stamp
    val laggingCol = lag(col("data"), 1).over(window)
    val laggingColtime = lag(col("timestamp"), 1).over(window)

    //checking there is any diff compared to previous data
    val Difference = col("data") - col("LastData")

    // Creating enriched table with lag sensor data , the difference between them and lag time stamp
    val enriched_tb = load_tb.withColumn("LastData", laggingCol)
      .withColumn("start_date", laggingColtime)
      .withColumn("Diff", Difference)

    //filtering out rows with changes in sensor data for reporting
    val report_tb = enriched_tb.filter(enriched_tb("Diff") === 1 || enriched_tb("Diff") === -1)
      .withColumnRenamed("timestamp", "end_date")
      .select("Sensor", "Mnemonic", "data", "start_date", "end_date")

    return report_tb

  }

}
