package com.leon

import java.io.PrintWriter
import java.text.{SimpleDateFormat, DateFormatSymbols}
import java.util.{Locale, Date}

import org.apache.hadoop.hbase.client._

import scala.collection.mutable

//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
//import org.apache.spark._

/**
  * Created by leon on 2016/3/16.
  */
object HbaseTest {
  def main(args : Array[String]) : Unit = {
    println("Hi, this is hbase test")

    val day = "20160411"
    val out = new PrintWriter("d://workroom//testroom//" + day+ ".csv")
    out.println("sn,nginxVV,videoVV,tagTV,tagTC,linkTC")

    val ts = getTS(day)
    val nginxVV = getHRecord(ts(0), "linz:nginx", "global", "realtime", "vv1m"+day)
    val videoVV = getHRecord(ts(0), "linz:videos", "global", "realtime", "vv1m"+day)
    val tagTV = getHRecord(ts(0), "linz:tags", "global", "realtime", "tv1m"+day)
    val tagTC = getHRecord(ts(0), "linz:tags", "global", "realtime", "tc1m"+day)
    val linkTC = getHRecord(ts(0), "linz:links", "global", "realtime", "tc1m"+day)

    val result = new mutable.HashMap[Long, (Long, Long, Long, Long, Long, Long)]
    val dft = (0, 0, 0, 0, 0, 0)
    for ( h <- 0 to 23 ) {
      val rcd = h + "," + nginxVV.getOrElse(h, 0) + "," + videoVV.getOrElse(h, 0) + "," + tagTV.getOrElse(h, 0) +
        "," + tagTC.getOrElse(h, 0) + "," + linkTC.getOrElse(h, 0)
      out.println(rcd)
      println(rcd)
    }
    out.close()
  }

  def getTS(time: String) : IndexedSeq[Long] = {
    val sym = new DateFormatSymbols(Locale.CHINA);
    val x1 = new SimpleDateFormat("yyyyMMdd", sym)
    val d = x1.parse(time)
    val start = d.getTime
    val end = d.getTime + (1440-1)*60000
    val tsVec = for ( ts <- start to end if (ts%60000)==0) yield ts
    tsVec
  }

  def getRecord(tableName:String, rowName:String, familyName:String, qulifiedName:String): mutable.HashMap[Long, Long] = {
    val nginxMap = new mutable.HashMap[Long, Long]

    val (hbaseconn, table) = Hbase.initHbase(tableName)
    val g = new Get(rowName.getBytes)
    g.setMaxVersions
    g.addColumn(familyName.getBytes, qulifiedName.getBytes)
    val result = table.get(g)
    val r0 = result.rawCells
    println(r0.size)
    for ( r1 <- r0 ) {
//      println(r1)
//      println(Bytes.toString(r1.getValue) + " " + r1.getTimestamp)
      nginxMap(r1.getTimestamp) = Bytes.toString(r1.getValue).toLong
    }
    table.close()
    hbaseconn.close()
    nginxMap
  }

  def getHRecord(baseTS:Long, tableName:String, rowName:String, familyName:String, qulifiedName:String): mutable.HashMap[Long, Long] = {
    val hourMap = new mutable.HashMap[Long, Long]
    val minuteMap = getRecord(tableName, rowName, familyName, qulifiedName)
    minuteMap.foreach(m => {
      val h = ((m._1 - baseTS) / 60000) / 60
      hourMap(h) = hourMap.getOrElse(h, 0L) + m._2
    })
    hourMap
  }
}
