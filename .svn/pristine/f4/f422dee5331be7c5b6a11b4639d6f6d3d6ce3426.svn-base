package com.humedica.mercury.etl.test

import java.io.File
import java.nio.file.{Files, Paths}
import com.humedica.mercury.etl.core.engine.{Constants, Engine}
import com.humedica.mercury.etl.test.support.{EntitySourceGraph, EntitySourceSummary}
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DocGenSpec extends FunSpec  {

  val dummyConfig = Map(
    Constants.PACKAGE_ROOT -> "com.humedica.mercury.etl",
    Constants.GROUP -> "H000000",
    Constants.CLIENT_DS_ID -> "-1",
    Constants.CLIENT_DS_NAME -> ""
  )

  val docRoot = new File("./target/mercury-shared")
  val docOut = new File(docRoot, "mercury-spec-doc.html")

  describe("Documentation generation") {
    it(s"Creates an ETL implementation summary at [${docOut.getAbsolutePath}].") {
      docRoot.mkdirs()
      val sources = Engine.sources.root()
      val g = new EntitySourceGraph(dummyConfig).apply(sources)
      val summary = new EntitySourceSummary(dummyConfig, g).apply(sources)
      Files.write(docOut.toPath, summary.toString.getBytes)
    }
  }
}
