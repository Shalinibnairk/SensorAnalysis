package com.sensor

import com.sensor.utilities.utilityfunctions


object Sensor {

  def main(args: Array[String]): Unit = {

    //In production environment the input file will be some other place not necessarily inside resource folder
    val path = getClass().getResource("/sensor").getFile

    if (utilityfunctions.validatePath(path)) {
      val spark = utilityfunctions.createSession

      val load_tb = utilityfunctions.sparkfilereader(spark, path)

      //.show wont be using in production environment, have to write the data to some database
      Triggering.identifyTrigger(load_tb).show()

    }
  }

}
