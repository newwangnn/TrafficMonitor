package com.msb.monitor.analysis

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.msb.monitor.constant.{AvgSpeedInfo, TrafficInfo}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MonitorTopnAnalysis {

  def myWinDate(args: Long,msg:String): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = new Date(args)
    println("-------------------------")
    println(msg)
    println(date) //Tue Sep 15 08:00:07 CST 2020
//    println(format.format(date))  //2020-09-15
  }

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop52:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"msb_008")

//    val stream1: DataStream[TrafficInfo] = env.addSource(new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(), prop).setStartFromEarliest())
      val stream1: DataStream[TrafficInfo] =env.socketTextStream("hadoop52",8888)
      .map(line => {
        val strs: Array[String] = line.split(",")
        new TrafficInfo(strs(0).toLong, strs(1), strs(2), strs(3), strs(4).toDouble, strs(5), strs(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficInfo](Time.seconds(5)) {
        override def extractTimestamp(element: TrafficInfo): Long = {
          myWinDate(element.actionTime,s"-----stream1 源数据-----${element}---")
          element.actionTime
        }
      })
    stream1.keyBy(_.monitorId)
        .timeWindow(Time.minutes(5),Time.minutes(2))
        .aggregate(new AggregateFunction[TrafficInfo,(Double,Int),(Double,Int)] {
          override def createAccumulator(): (Double, Int) = (0,0)

          override def add(value: TrafficInfo, accumulator: (Double, Int)): (Double, Int) = (accumulator._1+value.speed,accumulator._2+1)

          override def getResult(accumulator: (Double, Int)): (Double, Int) = accumulator

          override def merge(a: (Double, Int), b: (Double, Int)): (Double, Int) = (a._1+b._1,a._2+b._2)
        },(k:String, w:TimeWindow, iter:Iterable[(Double, Int)],coll: Collector[AvgSpeedInfo])=>{

          val last: (Double, Int) = iter.last
          val speed: Double = (last._1/last._2).formatted("%.2f").toDouble
        val info = new AvgSpeedInfo(w.getStart,w.getEnd,k,speed,last._2)
          myWinDate(w.getStart,s"-----stream2 滑动窗口开始------${info}--")
          myWinDate(w.getEnd,"-----stream2 滑动窗口结束--------")
          coll.collect(info)

    })
        .assignAscendingTimestamps(_.end)
        .timeWindowAll(Time.minutes(1))//窗口2 ，负责对窗口1输出的数据进行排序取TopN
      .apply(new AllWindowFunction[AvgSpeedInfo,String,TimeWindow] {
          override def apply(window: TimeWindow, input: Iterable[AvgSpeedInfo], out: Collector[String]): Unit = {
            //map集合，保存每个卡口的车辆数量,有可能在窗口1多次触发的时候（AllowedLateness），同一个卡口会有多条数据，留下车辆数量最多的
            //val map: Seq[(String, AvgSpeedInfo)] => mutable.Map[String, AvgSpeedInfo] = scala.collection.mutable.Map[String,AvgSpeedInfo]
            //当前处理数据迟到，采用的是Watermark，不需要map去重
              val infoes: List[AvgSpeedInfo] = input.toList.sortBy(_.carCount).reverse.take(3) //降序排序取Top3
              val builder = new StringBuilder
            builder.append(s"在窗口‘${infoes(0).start}---${infoes(0).end}’时间范围内，整个城市最堵的前3卡口是：\n\r")
            infoes.foreach(avgSpeedinfo=>{
              builder.append(s"卡口：${avgSpeedinfo.monitorId}，经过的车辆数量：${avgSpeedinfo.carCount},平均车速：${avgSpeedinfo.avgSpeed} \n\r")
            })
            myWinDate(window.getStart,s"-----stream3 TopN滚动窗口开始------${builder.toString()}--")
            myWinDate(window.getEnd,"-----stream3 TopN滚动窗口结束--------")
            out.collect(builder.toString())
          }
        })
        .print()
    env.execute()
  }

}
