package com.humedica.mercury.etl.core.apps

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.slf4j._
import java.time._
import java.time.format.DateTimeFormatter
import java.util.function.Predicate
import java.util.Objects.requireNonNull
import scala.collection.mutable.ListBuffer

object HdfsCopy {

  private val log = LoggerFactory.getLogger(getClass)

  def apply(fs: FileSystem, startYyyyMmDd: String, endYyyyMmDd: String,
            matchCriteria: Predicate[LocatedFileStatus], in: Path, out: Path): Unit = {
    requireNonNull(fs, "Missing HDFS access reference")
    requireNonNull(startYyyyMmDd, "Missing start date")
    requireNonNull(endYyyyMmDd, "Missing end date")
    requireNonNull(in, "Missing input folder location")
    requireNonNull(out, "Missing output folder location")
    require(fs.exists(in), s"Invalid path: [$in]")
    require(fs.isDirectory(in), s"Invalid path: [$in]")
    require(fs.exists(out), s"Invalid path: [$out]")
    require(fs.isDirectory(out), s"Invalid path: [$out]")

    val t0 = parseYyyyMmDd(startYyyyMmDd)
    val t1 = parseYyyyMmDd(endYyyyMmDd)
    val t0Ms = t0.toInstant.toEpochMilli
    val t1Ms = t1.toInstant.toEpochMilli
    val it = fs.listFiles(in, true)
    val entries = new ListBuffer[LocatedFileStatus]
    while ({it.hasNext}) entries += it.next

    entries.filter(fst => fst.isFile)
      .filter(fst => fst.getModificationTime >= t0Ms)
      .filter(fst => fst.getModificationTime <= t1Ms)
      .filter(fst => !fst.getPath.getName.equals("HISTORY"))
      .filter(fst => matchCriteria.test(fst)).foreach(fst => executeCopy(fst, out, fs))

    entries.filter(fst => fst.isFile)
      .filter(fst => fst.getPath.getName.contains("_ZH_"))
      .foreach(fst => executeCopy(fst, out, fs))

    entries.filter(fst => fst.isFile)
      .filter(fst => fst.getPath.getName.contains("EMPTY_FILE"))
      .foreach(fst => executeCopy(fst, out, fs))
  }

  def executeCopy(fst: LocatedFileStatus, out: Path, fs: FileSystem): Unit = {
    val p0 = new Path(out,
      new Path(fst.getPath.getParent.getParent.getParent.getName,
        new Path(fst.getPath.getParent.getParent.getName,
          new Path(fst.getPath.getParent.getName))))
    log.info(s"Copying [$fst] to [$p0]")
    if (!fs.exists(p0)) fs.mkdirs(p0)
    FileUtil.copy(fs, fst.getPath, fs, p0, false, fs.getConf)
  }

  def fileNameContains(groupId: String): Predicate[LocatedFileStatus] = new Predicate[LocatedFileStatus] {
    override def test(t: LocatedFileStatus): Boolean =
      t.getPath.getName.contains(groupId) // sigh...
  }

  def main(farts: Array[String]): Unit = {
    val spark = SparkSession.builder.appName(getClass.getCanonicalName).getOrCreate
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    HdfsCopy.apply(fs, System.getProperty("dcp.startDate"),
      System.getProperty("dcp.endDate"), fileNameContains(
        System.getProperty("dcp.groupId")),
      new Path(System.getProperty("dcp.input.location")),
      new Path(System.getProperty("dcp.output.location")))
  }

  private def parseYyyyMmDd(in: String) =
    LocalDate.parse(in, DateTimeFormatter.ofPattern("yyyyMMdd"))
      .atStartOfDay(ZoneId.of("UTC"))
}
