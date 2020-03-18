package com.humedica.mercury.toolkit

import org.apache.spark.repl.Main

object MtkRepl {

  def main(args: Array[String]): Unit = {
    val mtkLoop = new MtkILoop
    val doMainO = Main.getClass.getDeclaredMethods.find(_.getName == "doMain")
    require(doMainO.isDefined)
    val doMain = doMainO.get
    doMain.setAccessible(true)
    doMain.invoke(Main, args, mtkLoop)
  }

}
