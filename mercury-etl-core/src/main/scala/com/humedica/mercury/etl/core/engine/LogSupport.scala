package com.humedica.mercury.etl.core.engine

import java.io.OutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j._

object LogSupport {

  def traceToConsole(): Appender = {
    val mercuryLog = Logger.getLogger("com.humedica")
    val consoleAppender = new ConsoleAppender()
    mercuryLog.setLevel(Level.TRACE)
    mercuryLog.addAppender(consoleAppender)
    consoleAppender
  }

  def traceTo(target: OutputStream, callback: () => Unit): Unit = {
    val mercuryLogger = Logger.getLogger("com.humedica")
    val streamAppender = new WriterAppender(new PatternLayout(), target)
    mercuryLogger.setLevel(Level.TRACE)
    mercuryLogger.addAppender(streamAppender)
    callback()
    mercuryLogger.removeAppender(streamAppender)
  }

  def traceTo(target: Path, fs: FileSystem, callback: () => Unit): Unit = {
    val outStream = fs.create(target)
    traceTo(outStream, callback)
    outStream.close()
  }
}
