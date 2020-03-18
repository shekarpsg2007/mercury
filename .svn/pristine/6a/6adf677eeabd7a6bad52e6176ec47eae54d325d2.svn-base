package com.humedica.mercury.toolkit

import java.util.jar.{JarFile, Manifest}
import com.humedica.mercury.etl.core.engine.LogSupport
import org.apache.spark.repl.SparkILoop
import scala.util.Properties.{javaVersion, javaVmName, versionString}

class MtkILoop extends SparkILoop {

  override def processLine(line: String): Boolean = super.processLine(line)

  override def initializeSpark(): Unit = {
    super.initializeSpark()
    LogSupport.traceToConsole()
    intp.beQuietDuring {
      processLine("import org.apache.spark.sql.DataFrame")
      processLine("import org.apache.spark.sql.functions._")
      processLine("import org.apache.spark.sql.expressions.Window")
      processLine("import com.humedica.mercury.etl.core.engine.{Engine, EntitySource}")
      processLine("import com.humedica.mercury.etl.core.engine.Constants._")
      processLine("import com.humedica.mercury.etl.core.engine.Functions._")
      processLine("import com.humedica.mercury.etl.core.engine.Types._")
      processLine("import com.humedica.mercury.etl.core.engine.Metrics._")
      processLine("import com.humedica.mercury.toolkit.impl.Toolkit._")
      processLine("import com.humedica.mercury.toolkit.impl.Toolkit")
      processLine("import com.humedica.mercury.toolkit.impl.Diff._")

      processLine("spark.conf.set(\"spark.sql.caseSensitive\", \"false\")")
      processLine("Toolkit.setSession(spark)")
      processLine("val sqlContext = new org.apache.spark.sql.SQLContext(sc)")
      processLine("val default_dataset = spark.sparkContext.getConf.get(\"spark.driver.args\")")
      processLine("if (default_dataset != \"\") { use(default_dataset) }")
      processLine("val mtk = Toolkit")
      processLine("println(\"MTK available as 'mtk'.\")")
      replayCommandStack = Nil
    }
  }

  override def printWelcome(): Unit = {
    import org.apache.spark.SPARK_VERSION
    echo("""Mercury ETL Toolkit :: %s
      ___       ___       ___
     /\__\     /\  \     /\__\
    /::L_L_    \:\  \   /:/ _/_
   /:/L:\__\   /::\__\ /::-"\__\
   \/_/:/  /  /:/\/__/ \;:;-",-"
     /:/  /   \/__/     |:|  |
     \/__/               \|__|  version %s
         """.format(implVersionAndBuildNumber(), SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  private def implVersionAndBuildNumber(): String = {
    var result = "SNAPSHOT"
    try {
      val url = getClass.getClassLoader.getResource(JarFile.MANIFEST_NAME)
      val manifest = new Manifest(url.openStream)
      val bn = manifest.getMainAttributes.getValue("buildNumber")
      val ver = manifest.getMainAttributes.getValue("Implementation-Version")
      result = s"$ver Build number [$bn]"
    } catch { case e: Exception => }
    result
  }
}
