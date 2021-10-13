package com.msb.monitor.warning

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.Properties

import com.msb.monitor.constant.{TrackInfo, TrafficInfo, ViolationInfo}
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerConfig


import scala.collection.mutable

object CarViolationTrackAnalysis {


  def main(args: Array[String]): Unit = {
    val configuration: conf.Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "hadoop52,hadoop53,hadoop54")
    val conn: HConnection = HConnectionManager.createConnection(configuration)

//    val info = new TrackInfo("皖J31915", 1600152545000L, "5716", "45", 35.0)
//    var put = new Put(Bytes.toBytes("row_" + info.car + "_" + (Long.MaxValue - info.actionTime)))
//    put.add("cf1".getBytes, "car".getBytes, info.car.getBytes)
//    put.add("cf1".getBytes, "actionTime".getBytes, Bytes.toBytes(info.actionTime))
//    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("monitorId"), Bytes.toBytes(info.monitorId))
//    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("roadId"), Bytes.toBytes(info.roadId))
//    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("speed"), Bytes.toBytes(info.speed))

    val table: HTableInterface = conn.getTable("my_trackinfo")
//    table.put(put)
    val get = new Get(Bytes.toBytes("r1"))
    get.addFamily(Bytes.toBytes("cf1"))
    val  result: Result = table.get(get)

    println(s"----invoke hbase ::  ${result}")
    table.close()
    conn.close()
  }


  def main0(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop52:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "msb001")

    val stream: DataStream[TrackInfo] = env.addSource(new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(), prop).setStartFromEarliest())
      .map(line => {
        val strs: Array[String] = line.split(",")
        new TrafficInfo(strs(0).toLong, strs(1), strs(2), strs(3), strs(4).toDouble, strs(5), strs(6))
      })
      //      .assignAscendingTimestamps(_.actionTime)
      .filter(new MyRichFilterFunction)
      .map(trafficInfo => {
        new TrackInfo(trafficInfo.car, trafficInfo.actionTime, trafficInfo.monitorId, trafficInfo.roadId, trafficInfo.speed)
      })
    stream.countWindowAll(1)
      .apply((w: GlobalWindow, iter: Iterable[TrackInfo], out: Collector[java.util.List[Put]]) => {

        val list = new util.ArrayList[Put]()
        for (info <- iter) {
          var put = new Put(Bytes.toBytes("row_" + info.car + "_" + (Long.MaxValue - info.actionTime)))
          //            case class TrackInfo(car:String,actionTime:Long,monitorId:String,roadId:String,speed:Double)
          put.add("cf1".getBytes, "car".getBytes, info.car.getBytes)
          put.add("cf1".getBytes, "actionTime".getBytes, Bytes.toBytes(info.actionTime))
          put.add(Bytes.toBytes("cf1"), Bytes.toBytes("monitorId"), Bytes.toBytes(info.monitorId))
          put.add(Bytes.toBytes("cf1"), Bytes.toBytes("roadId"), Bytes.toBytes(info.roadId))
          put.add(Bytes.toBytes("cf1"), Bytes.toBytes("speed"), Bytes.toBytes(info.speed))
          list.add(put)
        }

        println(s"----window中的list[put] : ${list}")
        out.collect(list)
      })
      .addSink(new MyHbaseRichSinkFunction)

    env.execute()
  }

  class MyHbaseRichSinkFunction extends RichSinkFunction[java.util.List[Put]] {

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.hbase.HBaseConfiguration

    var conf: Configuration = _
    var conn: HConnection = _


    override def open(parameters: configuration.Configuration): Unit = {
      conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "hadoop52:2181")
      conn = HConnectionManager.createConnection(conf)
    }

    override def invoke(value: util.List[Put], context: SinkFunction.Context[_]): Unit = {
      val table: HTableInterface = conn.getTable("my_trackinfo")
      table.put(value)
      println(s"----invoke hbase ::  ${value}")
      table.close()
    }

    override def close(): Unit = {
      conn.close()
    }
  }

  class MyRichFilterFunction extends RichFilterFunction[TrafficInfo] {
    var map = new mutable.HashMap[String, ViolationInfo]()

    override def open(parameters: Configuration): Unit = {
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost/trafficmonitor?serverTimezone=UTC&useSSL=false", "root", "root")
      val sql = " SELECT car,violation,create_time FROM t_violation_list "
      val ps: PreparedStatement = conn.prepareStatement(sql)
      val rs: ResultSet = ps.executeQuery()
      while (rs.next()) {
        map.put(rs.getString(1), new ViolationInfo(rs.getString(1), rs.getString(2), rs.getLong(3)))
      }
      rs.close()
      ps.close()
      conn.close()
      println(s"-----mysql中加载的数据:${map}")
    }

    override def filter(value: TrafficInfo): Boolean = {
      val trafficmap = map.get(value.car)
      if (trafficmap.isEmpty) {
        false
      } else {
        true
      }

    }
  }

}
