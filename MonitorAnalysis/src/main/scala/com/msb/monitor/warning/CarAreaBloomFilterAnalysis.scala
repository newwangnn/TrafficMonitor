package com.msb.monitor.warning

import java.lang
import java.nio.charset.Charset
import java.util.Properties

import com.google.common.hash.Hashing
import com.msb.monitor.constant.TrafficInfo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.util.control.Breaks

object CarAreaBloomFilterAnalysis {

  def main1(args: Array[String]): Unit = {
    var jedis = new Jedis("hadoop55",6379)
    jedis.select(2)
    jedis.setbit("msb001",7,true)
    jedis.hset("t_mapkey_num","key1",1+"")

    jedis.close()
  }

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val keymaps = new mutable.HashMap[String,Int]()

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop52:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"msb003")

    env.addSource(new FlinkKafkaConsumer[String]("t_traffic_msb",new SimpleStringSchema(),prop).setStartFromEarliest())
      .map(line => {
        val strs: Array[String] = line.split(",")
        new TrafficInfo(strs(0).toLong, strs(1), strs(2), strs(3), strs(4).toDouble, strs(5), strs(6))
      })
      .keyBy(_.areaId)
      .timeWindow(Time.seconds(10))//模拟10分钟
      .trigger(new MyTrigger)
      .process(new ProcessWindowFunction[TrafficInfo,String,String,TimeWindow] {

        var jedis :Jedis = _
        var myBloom:MyBloomFilter = _

        override def open(parameters: Configuration): Unit = {
          jedis = new Jedis("hadoop55",6379)
          jedis.select(2)
          myBloom = new MyBloomFilter(1<<22)
        }

        override def process(key: String, context: Context, elements: Iterable[TrafficInfo], out: Collector[String]): Unit = {

          val start: Long = context.window.getStart
          val end: Long = context.window.getEnd
          val car: String = elements.last.car

          println(s"area:${key} :: start:${start}---end:${end}")

          var mapkey = key+"_"+end
          var count:Int = 0
          if(keymaps.contains(mapkey)){
           count =  keymaps.getOrElse(mapkey,0)
          }

          var repeated = true
          val breaks = new Breaks
          val arr: Array[Long] = myBloom.getOffset(car)
          breaks.breakable({
            for (elem <- arr) {
              val flag: lang.Boolean = jedis.getbit(mapkey,elem)

              if(!flag){
                repeated=false
                breaks.break()
              }
            }
          })

          if(!repeated){
            count+=1
            for (elem <- arr) {
              jedis.setbit(mapkey,elem,true)
            }
            keymaps.put(mapkey,count)

            jedis.hset("t_counter",mapkey,count+"")
          }

          out.collect(s"区域：${key} ::窗口开始时间 ${start} ::窗口结束时间 ${end} ::时间段内车辆共计 ${count} 辆")
        }
      })
//        .print()

    env.execute()

  }

  class MyTrigger extends Trigger[TrafficInfo,TimeWindow]{
    override def onElement(element: TrafficInfo, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

  class MyBloomFilter(len:Long) extends Serializable{

    def getOffset(car:String):Array[Long]={
      val arr = new Array[Long](2)

      var hashcode1: Long = getGoogleHash(car)
      if(hashcode1<0){
        hashcode1 = ~hashcode1
      }
      arr(0)=hashcode1%len

      var hashcode2: Int = car.hashCode
      if(hashcode2<0){
        hashcode2 = ~ hashcode2
      }
      arr(1)=hashcode2%len

      arr
    }

    def getGoogleHash(car:String):Long={
      Hashing.murmur3_128(1).hashString(car,Charset.forName("utf-8")).asLong()
    }
  }

}
