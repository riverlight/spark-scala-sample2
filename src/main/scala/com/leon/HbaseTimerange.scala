package com.leon

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by leon on 2016/4/18.
  */
object HbaseTimerange {
  def main(args: Array[String]) : Unit = {
    println("hi, this is hbase time-range test program")

    val (hbaseconn, table) = Hbase.initHbase("kk")
    val s = new Scan()
    s.addColumn("cf".getBytes, "qlf".getBytes)
    s.setMaxVersions(1440)
    s.setTimeRange(1, 1460972523589L+1)
    val scanner = table.getScanner(s)
    var r = scanner.next
    while (r!=null) {
      val r0 = r.rawCells
      val recordName = Bytes.toString(r.getRow)
      //println(Bytes.toString(r.getRow))
      for ( r1 <- r0 ) {
        //      println(r1)
        println(Bytes.toString(r1.getValue) + " " + r1.getTimestamp + " * " + Bytes.toString(r1.getRow))
        //nginxMap(r1.getTimestamp) = Bytes.toString(r1.getValue).toLong
        //recordMap(recordName) = Bytes.toString(r1.getValue).toLong + recordMap.getOrElse(recordName, 0L)
      }
      r = scanner.next
    }

    scanner.close()
    table.close()
    hbaseconn.close()
  }
}
