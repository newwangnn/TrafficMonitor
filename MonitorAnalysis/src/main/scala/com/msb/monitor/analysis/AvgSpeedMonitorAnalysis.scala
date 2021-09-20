package com.msb.monitor.analysis

import java.util.Properties

import com.msb.monitor.constant.{AvgSpeedInfo, TrafficInfo}
import com.msb.monitor.util.JdbcWriteDataSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
object AvgSpeedMonitorAnalysis {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop52:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"msb_005")

    val stream1: DataStream[TrafficInfo] = env.addSource(new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(), prop).setStartFromEarliest())
      .map(line => {
        val strs: Array[String] = line.split(",")
        //(actionTime:Long,monitorId:String,cameraId:String,car:String,speed:Double,roadId:String,areaId:String)
        new TrafficInfo(strs(0).toLong, strs(1), strs(2), strs(3), strs(4).toDouble, strs(5), strs(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficInfo](Time.seconds(5)) {
        override def extractTimestamp(element: TrafficInfo): Long = element.actionTime
      })
    val stream2: DataStream[AvgSpeedInfo] = stream1.keyBy(_.monitorId)
      .timeWindow(Time.minutes(5), Time.minutes(1))
      .aggregate(new AggregateFunction[TrafficInfo, (Double, Int), (Double, Int)] {
        override def createAccumulator(): (Double, Int) = (0, 0)

        override def add(value: TrafficInfo, acc: (Double, Int)): (Double, Int) = (acc._1 + value.speed, acc._2 + 1)

        //          (Start:Long,end:Long,monitorId:String,avgSpeed:Double,carCount:Int)
        override def getResult(accumulator: (Double, Int)): (Double, Int) = accumulator

        override def merge(a: (Double, Int), b: (Double, Int)): (Double, Int) = (a._1 + b._1, a._2 + b._2)
      }, (k: String, w: TimeWindow, iter: Iterable[(Double, Int)], col: Collector[AvgSpeedInfo]) => {
        val size: Int = iter.size
        println(s"-------size:$size")
        val last: (Double, Int) = iter.last
        val info = new AvgSpeedInfo(w.getStart, w.getEnd, k, last._1 / last._2, last._2)
        col.collect(info)
      })
    val sql="insert into t_average_speed(start_time,end_time,monitor_id,avg_speed,car_count ) values(?,?,?,?,?)"
    stream2.addSink(new JdbcWriteDataSource[AvgSpeedInfo](classOf[AvgSpeedInfo],sql))
    stream2.print()

    env.execute()
  }

}
