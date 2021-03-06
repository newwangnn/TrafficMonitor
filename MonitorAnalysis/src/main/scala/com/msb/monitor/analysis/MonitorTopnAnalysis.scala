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
          myWinDate(element.actionTime,s"-----stream1 ?????????-----${element}---")
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
          myWinDate(w.getStart,s"-----stream2 ??????????????????------${info}--")
          myWinDate(w.getEnd,"-----stream2 ??????????????????--------")
          coll.collect(info)

    })
        .assignAscendingTimestamps(_.end)
        .timeWindowAll(Time.minutes(1))//??????2 ??????????????????1??????????????????????????????TopN
      .apply(new AllWindowFunction[AvgSpeedInfo,String,TimeWindow] {
          override def apply(window: TimeWindow, input: Iterable[AvgSpeedInfo], out: Collector[String]): Unit = {
            //map??????????????????????????????????????????,??????????????????1????????????????????????AllowedLateness?????????????????????????????????????????????????????????????????????
            //val map: Seq[(String, AvgSpeedInfo)] => mutable.Map[String, AvgSpeedInfo] = scala.collection.mutable.Map[String,AvgSpeedInfo]
            //???????????????????????????????????????Watermark????????????map??????
              val infoes: List[AvgSpeedInfo] = input.toList.sortBy(_.carCount).reverse.take(3) //???????????????Top3
              val builder = new StringBuilder
            builder.append(s"????????????${infoes(0).start}---${infoes(0).end}?????????????????????????????????????????????3????????????\n\r")
            infoes.foreach(avgSpeedinfo=>{
              builder.append(s"?????????${avgSpeedinfo.monitorId}???????????????????????????${avgSpeedinfo.carCount},???????????????${avgSpeedinfo.avgSpeed} \n\r")
            })
            myWinDate(window.getStart,s"-----stream3 TopN??????????????????------${builder.toString()}--")
            myWinDate(window.getEnd,"-----stream3 TopN??????????????????--------")
            out.collect(builder.toString())
          }
        })
        .print()
    env.execute()
  }

}
