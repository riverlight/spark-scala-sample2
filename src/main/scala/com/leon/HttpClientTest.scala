package com.leon

import java.io._
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpPost, HttpGet}
import org.apache.http.impl.client.DefaultHttpClient
import scala.collection.mutable.StringBuilder
import scala.xml.XML


/**
  * Created by leon on 2016/3/16.
  */
object HttpClientTest {
  def main(args : Array[String]) : Unit = {
    println("Hi, this is http client test")

    // (1) get the content from the yahoo weather api url
    val content = getRestContent("http://weather.yahooapis.com/forecastrss?p=80020&u=f")

    println(content)

    // (2) convert it to xml
    val xml = XML.loadString(content)
    assert(xml.isInstanceOf[scala.xml.Elem])  // needed?

    // (3) search the xml for the nodes i want
    val temp = (xml \\ "channel" \\ "item" \ "condition" \ "@temp") text
    val text = (xml \\ "channel" \\ "item" \ "condition" \ "@text") text

    // (4) print the results
    val currentWeather = format("The current temperature is %s degrees, and the sky is %s.", temp, text.toLowerCase())
    println(currentWeather)

  }

  def getRestContent(url:String): String = {
//    val httpClient = new DefaultHttpClient()
//    val httpResponse = httpClient.execute(new HttpGet(url))
//    val entity = httpResponse.getEntity()
//    var content = ""
//    if (entity != null) {
//      val inputStream = entity.getContent()
//      content = io.Source.fromInputStream(inputStream).getLines.mkString
//      inputStream.close
//    }
//    httpClient.getConnectionManager().shutdown()
//    return content
    "kk"
  }

  def postRestContent(url:String) : String = {
    val httpClient = new DefaultHttpClient()
    val httpResponse = httpClient.execute(new HttpPost(url))
    "abc"
  }
}
