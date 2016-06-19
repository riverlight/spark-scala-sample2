package com.leon

import com.mongodb.ServerAddress
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCredential}
import com.mongodb.casbah.Imports._

/**
  * Created by leon on 2016/2/21.
  */
object MongoTest {
  def main(args: Array[String]): Unit = {
    println("Hi, this is mongo test program")

    val server = new ServerAddress("192.168.227.131", 27017)
    val credentials = MongoCredential.createMongoCRCredential("leon", "sca", "123".toArray)
    val mongoClient = MongoClient(server, List(credentials))
    val db = mongoClient("sca")
    db.collectionNames
    val mysca = db("leonsca")
//    val dbPair = MongoDBObject("hello" -> 200, "time" -> "20160221")
//    mysca.insert(dbPair)

    mysca.find().foreach(x => println(x))
    val query1 = MongoDBObject("time" -> "20160221a")
    println("----\n\n" + "findone" + "\n\n------\n\n")
    val f = mysca.findOne(query1)
    println(f)

    if (f!=None) {
      val dq = MongoDBObject("_id"->f.getOrElse(null)("_id"))
      mysca.remove(dq)
    }
    println("********\nhow many\n******")
    val many = mysca.find(query1)
    println(many.size)
    System.exit(0)
    //f("hello") += 33
//    val update1 = MongoDBObject("hello" -> 23)
//    val k = update1 ++ query1
//    mysca.update(query1, $inc ("hello" -> 100 ), true)
    val ins = MongoDBObject("hello"->125, "time"->"20160221")
    mysca.insert(ins)
  }
}
