package com.leon

import java.io.PrintWriter

import com.mongodb.ServerAddress
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCredential}
import org.bson.types.ObjectId

import scala.io.Source

/**
  * Created by leon on 2016/4/15.
  */
object Id2Title {
  val server = new ServerAddress("10.10.192.149", 27017)
  val credentials = MongoCredential.createMongoCRCredential("readuser", "videojj", "1cdff729ef3ed807e2b82105c320ca4b".toArray)
  val mongoClient = MongoClient(server, List(credentials))
  val db = mongoClient("videojj")
  db.collectionNames
  val dgsca = db("dgs")
  val videosca = db("videos")
  val tagsca = db("tags")

  def main(args : Array[String]) : Unit = {
    println("hi, this is a ID2TITLE program")

    val day = "20160417"
    dgId(day)
    videoId(day)

  }

  def dgId(day : String) : Unit = {
    val out = new PrintWriter("d://workroom//testroom//dg" + day + "rr.csv")

    var count = 0
    for ( line <- Source.fromFile("d://workroom//testroom//dg"+day+"r.csv").getLines()) {
      if (count==0) {
        out.println(line+",title,cat")
        count += 1
      } else {
        val strs = line.split(',')
        val id = strs(1).substring(1, strs(1).length-1)
        val tc = strs(2).toLong

        val q = MongoDBObject("_id" -> new ObjectId(id))
        if (tc>100) {
          val dg = dgsca.findOne(q)
          dg.foreach(x => {
            out.println(line + "," + x.get("title") + "," + x.get("cat"))
          })
        } else {
          out.println(line + "," + "unknown" + "," + -1)
        }

      }
    }
    out.close()
  }

  def videoId(day : String) : Unit = {
    val out = new PrintWriter("d://workroom//testroom//video" + day + "rr.csv")

    var count = 0
    for ( line <- Source.fromFile("d://workroom//testroom//video"+day+"r.csv").getLines()) {
      if (count==0) {
        out.println(line+",title")
        count += 1
      } else {
        val strs = line.split(',')
        val id = strs(1).substring(1, strs(1).length-1)
        val vv = strs(2).toLong

        val q = MongoDBObject("_id" -> new ObjectId(id))
        if (vv>10000) {
          val video = videosca.findOne(q)
          video.foreach(x => {
            out.println(line + "," + x.get("title"))
          })
        } else {
          out.println(line + "," + "NA")
        }

      }
    }
    out.close()
  }

}
