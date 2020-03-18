package com.humedica.mercury.etl.core.apps

import com.typesafe.config.{ConfigFactory, ConfigObject}
import scala.collection.JavaConversions._

object ConfigReader {

  def main(args: Array[String]): Unit = {
    println("Fetching...")
    datasets()
    datasets(emr = "asent")
    datasets(group = "H328218")
    datasets(emr = "asent", group = "H053731")
    val currentConfig = usedataset("H053731_ASENT")
    printConfig(currentConfig)
  }

  def usedataset(configName: String): Map[String, String] = {
    //todo for when config is not in resources:
    // val configFile = new File("path/configFile.conf")
    // val fileConfig = ConfigFactory.parseFile(configFile).getConfig(configName)
    //val config = ConfigFactory.load(fileConfig)
    val config = ConfigFactory.load("datasets.conf")
    val configMap = config.getConfig("Datasets." + configName).root.unwrapped().toMap
    configMap.mapValues(_.toString)
  }

  def printConfig(configMap: Map[String, String]): Unit = {
    configMap.keys.foreach { i =>
      println(i + "=" + configMap(i))
    }
  }

  //TODO DRY
  def datasets(emr: String = "", group: String = "") = {
    val configs = ConfigFactory.load("datasets.conf").getConfig("Datasets")
    val headingName = "NAME"
    val headingEMR = "EMR"
    val headingGroup = "GROUP"
    val headingClient = "CLIENT_DS_NAME"
    val headingEmrData = "EMR_DATA_ROOT"
    println(headingName.padTo(25, "-").mkString + " | "
      + headingEMR.padTo(10, "-").mkString + " | "
      + headingGroup.padTo(10, "-").mkString + " | "
      + headingClient.padTo(25, "-").mkString + " | "
      + headingEmrData.padTo(50, "-").mkString)
    val configMap = configs.root().map { case (name: String, configObject: ConfigObject) =>
      val eachConfig = configObject.toConfig
      val configEmr = eachConfig.getString("EMR")
      val configGroup = eachConfig.getString("GROUP")
      val configClientDsName = eachConfig.getString("CLIENT_DS_NAME")
      val configEmrDataroot = eachConfig.getString("EMR_DATA_ROOT")
      val filterEMR = if (emr == "" && group == "") {
        println(name.padTo(25, " ").mkString + " | "
          + configEmr.padTo(10, " ").mkString + " | "
          + configGroup.padTo(10, " ").mkString + " | "
          + configClientDsName.padTo(25, " ").mkString + " | "
          + configEmrDataroot.padTo(50, " ").mkString)
      } else if (configEmr == emr && configGroup == group) {
        println(name.padTo(25, " ").mkString + " | "
          + configEmr.padTo(10, " ").mkString + " | "
          + configGroup.padTo(10, " ").mkString + " | "
          + configClientDsName.padTo(25, " ").mkString + " | "
          + configEmrDataroot.padTo(50, " ").mkString)
      } else if (configEmr == emr && group == "") {
        println(name.padTo(25, " ").mkString + " | "
          + configEmr.padTo(10, " ").mkString + " | "
          + configGroup.padTo(10, " ").mkString + " | "
          + configClientDsName.padTo(25, " ").mkString + " | "
          + configEmrDataroot.padTo(50, " ").mkString)
      } else if (emr == "" && configGroup == group)
        println(name.padTo(25, " ").mkString + " | "
          + configEmr.padTo(10, " ").mkString + " | "
          + configGroup.padTo(10, " ").mkString + " | "
          + configClientDsName.padTo(25, " ").mkString + " | "
          + configEmrDataroot.padTo(50, " ").mkString)
    }
  }

  //TODO add currentdataset() functionality

}