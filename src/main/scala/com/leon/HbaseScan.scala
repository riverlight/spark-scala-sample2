package com.leon

import java.io.PrintWriter

import com.mongodb.ServerAddress
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCredential}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Scan, Table, ConnectionFactory, Connection}
import org.bson.types.ObjectId

import scala.collection.mutable


/**
  * Created by leon on 2016/4/11.
  */
object HbaseScan {
  def main(args: Array[String]) : Unit = {
    println("Hbase scan test")

    //scanAll()

    // video
    val day = "20160418"
    scanVideo(day)
    scanDg(day)
  }

  def scanVideo(day: String) : Unit = {
    val vv = scanRecord("linz:videos", "realtime", "vv1m"+day)
    val tv = scanRecord("linz:videos", "realtime", "tv1m"+day)
    val tc = scanRecord("linz:videos", "realtime", "tc1m"+day)
    val lc = scanRecord("linz:videos", "realtime", "lc1m"+day)
    val out = new PrintWriter("d://workroom//testroom//"+ "video" + day+ ".csv")
    out.println("id,vv,tv,tc,lc")
    vv.foreach(e => {
      val id = e._1
      out.println(id + "," + vv.getOrElse(id, 0) + "," + tv.getOrElse(id, 0) + "," + tc.getOrElse(id, 0) + "," + lc.getOrElse(id, 0))
    })
    out.close()
  }

  def scanDg(day: String) : Unit = {
    val tc = scanRecord("linz:ads", "realtime", "tc1m"+day)
    val lc = scanRecord("linz:ads", "realtime", "lc1m"+day)
    val dura = scanRecord("linz:ads", "realtime", "dura" + day)
    val cc = scanRecord("linz:ads", "realtime", "cc1m" + day)
    val out = new PrintWriter("d://workroom//testroom//"+ "dg" + day+ ".csv")
    out.println("id,tc,lc,cc,dura")
    tc.foreach(e => {
      val id = e._1
      out.println(id + "," + tc.getOrElse(id, 0) + "," + lc.getOrElse(id, 0) + "," + cc.getOrElse(id, 0) + "," + dura.getOrElse(id, 0))

    })
    out.close()
  }

  def scanAll() : Unit = {
    val day = "20160412"
    println("videos")
    scan("linz:videos", "v", "vv1m", day)
    scan("linz:videos", "v", "tv1m", day)
    scan("linz:videos", "v", "tc1m", day)
    scan("linz:videos", "v", "lc1m", day)
    println("dgs")
    scan("linz:ads", "d", "tc1m", day)
    scan("linz:ads", "d", "lc1m", day)
    println("over~~~")
  }

  def scan(tableName:String, fprefix: String, prefix: String, day: String) : Unit = {
    val vv = scanRecord(tableName, "realtime", prefix+day)
    val out = new PrintWriter("d://workroom//testroom//"+ fprefix + prefix + day+ ".csv")
    out.println("id,"+prefix)
    vv.foreach(e => out.println(e._1 + "," + e._2))
    out.close()
  }

  def scanRecord(tableName:String, familyName:String, qulifiedName:String) : mutable.HashMap[String, Long] = {
    val recordMap = new mutable.HashMap[String, Long]
    val (hbaseconn, table) = Hbase.initHbase(tableName)
    val s = new Scan()
    s.addColumn(familyName.getBytes, qulifiedName.getBytes)
    s.setMaxVersions(1440)
    val scanner = table.getScanner(s)
    var r = scanner.next
    while (r!=null) {
      val r0 = r.rawCells
      val recordName = Bytes.toString(r.getRow)
      //println(Bytes.toString(r.getRow))
      for ( r1 <- r0 ) {
        //      println(r1)
        //println(Bytes.toString(r1.getValue) + " " + r1.getTimestamp + " * " + Bytes.toString(r1.getRow))
        //nginxMap(r1.getTimestamp) = Bytes.toString(r1.getValue).toLong
        recordMap(recordName) = Bytes.toString(r1.getValue).toLong + recordMap.getOrElse(recordName, 0L)
      }
      r = scanner.next
    }

    scanner.close()
    table.close()
    hbaseconn.close()
    recordMap
  }
}
