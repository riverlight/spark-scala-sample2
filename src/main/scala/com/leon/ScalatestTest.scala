package com.leon

import collection.mutable.Stack
import org.scalatest.FlatSpec

import scala.collection.mutable
import play.api.libs.json


/**
  * Created by leon on 2016/3/8.
  */
class ScalatestTest extends FlatSpec {

  "A stack" should "pop values in LIFO " in {
    val stack = new mutable.Stack[Int]()
    stack.push(10)
    stack.push(20)
    assert(stack.pop==20)
    assert(stack.pop==10)
  }
}
