package com.msb.monitor.analysis

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.msb.monitor.constant.{RepetitionCarWarning, TrafficInfo, ViolationInfo}
import com.msb.monitor.util.JdbcWriteDataSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RepeatCarAnalysis {

  def main0(args: Array[String]): Unit = {
//    1600165592000
//val format = new SimpleDateFormat("yyyy-MM-dd")
//    val date = new Date(1600165530000L)
//    println(date) //Tue Sep 15 18:25:30 CST 2020

//       var sdf = new SimpleDateFormat("yyyy-MM-dd")
    //      val day: Date = sdf.parse(sdf.format(new Date(info.action_time))) //当前的0时0分0秒

    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = new Date(1632543058889L)
    val str: String = format.format(date)
    val d1: Date = format.parse(str)
    println(d1)



  }

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop52:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"msb_002")

//    val stream: DataStream[RepetitionCarWarning] = env.addSource(new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(), prop).setStartFromEarliest())
    val stream: DataStream[RepetitionCarWarning] = env.socketTextStream("hadoop52",8888)
      .map(line => {
        val strs: Array[String] = line.split(",")
        new TrafficInfo(strs(0).toLong, strs(1), strs(2), strs(3), strs(4).toDouble, strs(5), strs(6))
      })
      .keyBy(_.car)
      .process(new KeyedProcessFunction[String, TrafficInfo, RepetitionCarWarning] {
        lazy val trafficState: ValueState[TrafficInfo] = getRuntimeContext.getState(new ValueStateDescriptor[TrafficInfo]("violation", classOf[TrafficInfo]))

        override def processElement(value: TrafficInfo, ctx: KeyedProcessFunction[String, TrafficInfo, RepetitionCarWarning]#Context, out: Collector[RepetitionCarWarning]): Unit = {
          val info: TrafficInfo = trafficState.value()
          if (info == null) {
            trafficState.update(value)
          } else {
            val actiontime: Long = info.actionTime
            val firstTime: Long = value.actionTime
            var outTime = (actiontime - firstTime).abs / 1000
            if (outTime < 10) { //10秒内存在套牌车可能
              val warningInfo = new RepetitionCarWarning(value.car, if (firstTime > actiontime) info.cameraId else value.cameraId,
                if (firstTime > actiontime) value.cameraId else info.cameraId,
                "存在套牌嫌疑", System.currentTimeMillis())
              out.collect(warningInfo)

            } else {
              trafficState.update(value)
            }

          }
        }
      })
    val sql = "INSERT INTO t_repeat_car(car,first_monitor_id,second_monitor_id,msg,create_time) VALUES(?,?,?,?,?)"
    stream.addSink(new JdbcWriteDataSource[RepetitionCarWarning](classOf[RepetitionCarWarning],sql))


    env.execute()
  }

}
