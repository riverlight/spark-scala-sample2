package com.leon

import play.api.libs.json._
import dispatch.{Http,url}
import Http._

/**
  * Created by leon on 2016/3/16.
  */
object JsonTest {
  def main(args : Array[String]) : Unit = {
    println("Hi, this is a play json test")

    val jsonString = """{ "key1" : "val1", "key2" : 10, "key3" : { "key4" : "hello", "key5" : 8.9 } }"""
    val json = Json.parse(jsonString)
    println(json)
    println((json \ "key1").asOpt[String].getOrElse("haha"))

    val str1 = Json.stringify(json)
    println(str1)

    val jsonObj = Json.toJson(
      Map(
        "name" -> Json.toJson("leon"),
        "age" -> Json.toJson(37),
        "user" ->
          Json.toJson(
            Map(
              "name"-> Json.toJson("kim"),
              "age" -> Json.toJson(34)
            )
          )

      )
    )

    val str2 : String = jsonObj.toString()
    println(str2)

    //Http(url("http://www.baidu.com") OK System.out)
  }

}
