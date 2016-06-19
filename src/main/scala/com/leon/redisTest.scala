package com.leon

import redis.clients.jedis.Jedis


/**
  * Created by leon on 2016/1/26.
  */


object redisTest {

  def main(args : Array[String]) : Unit = {
    println("Hi, this is a redisTest program!")

    val host = "10.10.81.101"
    val port = 7001
    val jedis = new Jedis(host, port)
    //jedis.hincrBy("leon", "0", 1)
    val len = jedis.llen("haha")
    println(len)
    if (len>10) jedis.rpop("haha")
    jedis.lpush("haha", "kk")
    jedis.close()
  }
}

