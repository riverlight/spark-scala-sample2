package com.leon

import com.mongodb.{DBObject, ServerAddress}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCredential}
import org.bson.types.ObjectId
import org.bson.{BSON, BSONObject}

/**
  * Created by leon on 2016/4/15.
  */
object MongoT1 {
  def main(args: Array[String]): Unit = {
    println("Hi, this is mongo test1 program")

    val server = new ServerAddress("10.10.192.149", 27017)
    val credentials = MongoCredential.createMongoCRCredential("readuser", "videojj", "1cdff729ef3ed807e2b82105c320ca4b".toArray)
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient("videojj")
    db.collectionNames
    val dgsca = db("dgs")
    //    val dbPair = MongoDBObject("hello" -> 200, "time" -> "20160221")
    //    mysca.insert(dbPair)

//    val mm  =  { "id" -> "54f6dd489926c69306c79ea1"}
    //val query1 = MongoDBObject("interacts"-> 3307)
    val obj = new ObjectId("54f6dd489926c69306c79ea1")
    val query1 = MongoDBObject("_id" -> obj)
    val q = dgsca.findOne(query1)
    println("************")
    q.foreach(x => println(x.get("_id")))

  }
}
