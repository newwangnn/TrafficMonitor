package com.msb.monitor.warning

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.msb.monitor.constant.{DangerousDrivingWarning, MonitorInfo, OutOfLimitSpeedInfo, TrafficInfo}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

object DangerousDriverWarningAnalysis {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop52:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "msb_04")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

//    val stream: DataStream[OutOfLimitSpeedInfo] = env.addSource(new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema, prop))
    val stream: DataStream[OutOfLimitSpeedInfo] = env.socketTextStream("hadoop52", 8888)
      .map(line => {
        val strs: Array[String] = line.split(",")
        new TrafficInfo(strs(0).toLong, strs(1), strs(2), strs(3), strs(4).toDouble, strs(5), strs(6))
      })
      .map(new MyRichMapFunction(60))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OutOfLimitSpeedInfo](Time.seconds(5)) {
        override def extractTimestamp(element: OutOfLimitSpeedInfo): Long = element.actionTime
      })

    val pattern: Pattern[OutOfLimitSpeedInfo, OutOfLimitSpeedInfo] = Pattern.begin[OutOfLimitSpeedInfo]("first")
      .where(t => {
        t.limitSpeed * 1.2 < t.realSpeed
      })
      .timesOrMore(2)
      .within(Time.minutes(2))
//      .greedy

    val cep: PatternStream[OutOfLimitSpeedInfo] = CEP.pattern(stream.keyBy(_.car), pattern)
    cep.select(map => {
      println(s"cep select ${map}")
      val list: List[OutOfLimitSpeedInfo] = map.get("first").get.toList
      val builder = new mutable.StringBuilder()
      val car: String = list.head.car
      builder.append(s"危险驾驶：${car} : ")
      list.foreach(f => {
        builder.append(s"危险驾驶车辆：${f} --")
      })
      new DangerousDrivingWarning(car,builder.toString(),list.head.actionTime,list.head.realSpeed,list.head.limitSpeed)
    })
      .print()

    env.execute()

  }

  class MyRichMapFunction(defaultSpeed: Int) extends RichMapFunction[TrafficInfo, OutOfLimitSpeedInfo] {

    var map = scala.collection.mutable.Map[String, MonitorInfo]()

    override def open(parameters: Configuration): Unit = {
      println("MyRichMapFunction -------(1)------")
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/trafficmonitor?serverTimezone=UTC&useSSL=false", "root", "root")
      val ps: PreparedStatement = conn.prepareStatement(" SELECT monitor_id,road_id,speed_limit,area_id FROM t_monitor_info ")
      val rs: ResultSet = ps.executeQuery()
      while (rs.next()) {
        val monitorId: String = rs.getString(1)
        val roadId: String = rs.getString(2)
        val limitSpeed: Int = rs.getInt(3)
        val areaId: String = rs.getString(4)
        val info = MonitorInfo(monitorId, roadId, limitSpeed, areaId)
        map.put(monitorId, info)
      }
      rs.close()
      ps.close()
      conn.close()
    }

    override def map(value: TrafficInfo): OutOfLimitSpeedInfo = {
      val monitor: MonitorInfo = map.getOrElse(value.monitorId, new MonitorInfo(value.monitorId, value.roadId, defaultSpeed, value.areaId))
      new OutOfLimitSpeedInfo(value.car, monitor.monitorId, monitor.roadId, value.speed, monitor.limitSpeed, value.actionTime)
    }
  }

}
