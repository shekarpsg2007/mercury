package com.humedica.mercury.etl.core.util

import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.RemoteIterator
import scala.collection.mutable.ListBuffer

object RemoteIteratorToList {
  def apply(ri: RemoteIterator[LocatedFileStatus]): ListBuffer[LocatedFileStatus] = {
    val l0 = new ListBuffer[LocatedFileStatus]
    while ({ri.hasNext}) l0 += ri.next
    l0
  }
}
