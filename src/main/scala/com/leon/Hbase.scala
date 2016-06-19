package com.leon

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Table, ConnectionFactory, Connection}

/**
  * Created by leon on 2016/4/5.
  */
object Hbase {
  def initHbase(table_name : String): (Connection, Table) = {
    val hbaseconf = HBaseConfiguration.create
    hbaseconf.set("hbase.zookeeper.quorum", "uhadoop-jw0dez-master2,uhadoop-jw0dez-core1,uhadoop-jw0dez-master1")
    //hbaseconf.set("hbase.master", "10.10.242.147:60010, 10.10.228.187:60010,10.10.217.87:60010")
    hbaseconf.set("hbase.master", "10.10.242.147:60010")

    val hbaseconn=ConnectionFactory.createConnection(hbaseconf)
    val admin = hbaseconn.getAdmin
    val userTable=TableName.valueOf(table_name)
    val table=hbaseconn.getTable(userTable)

    (hbaseconn, table)
  }
}
