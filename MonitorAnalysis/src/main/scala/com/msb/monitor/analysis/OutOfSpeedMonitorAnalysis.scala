package com.msb.monitor.analysis

import java.util.Properties

import com.msb.monitor.constant.{GlobalConstants, MonitorInfo, OutOfLimitSpeedInfo, TrafficInfo}
import com.msb.monitor.util.{JdbcReadDataSource, JdbcWriteDataSource}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

object OutOfSpeedMonitorAnalysis {

  def main1(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //stream1海量的数据流，不可以存入广播状态流中
    //stream2 从Mysql数据库中读取的卡口限速信息，特点：数据量少，更新不频繁
    val sql = "select monitor_id,road_id,speed_limit,area_id from t_monitor_info where speed_limit > 0"
    //    val stream1: BroadcastStream[MonitorInfo] = env.addSource(new JdbcReadDataSource[MonitorInfo](classOf[MonitorInfo], sql))
    val stream2: BroadcastStream[MonitorInfo] = streamEnv.addSource(new JdbcReadDataSource[MonitorInfo](classOf[MonitorInfo],sql))
      .broadcast(GlobalConstants.MONITOR_STATE_DESCRIPTOR)

    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop52:9092,hadoop53:9092,hadoop54:9092")
    props.setProperty("group.id","msb_001")


    //创建一个Kafka的Source
    val stream1: DataStream[TrafficInfo] = streamEnv.addSource(
      new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(), props).setStartFromEarliest() //从第一行开始读取数据
    )
      //    val stream1: DataStream[TrafficInfo] = streamEnv.socketTextStream("hadoop101",9999)
      .map(line => {
      var arr = line.split(",")
      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    })



    //Flink中有connect ：内和外都可以  和join ：内连接
    stream1.connect(stream2)
      .process(new BroadcastProcessFunction[TrafficInfo,MonitorInfo,OutOfLimitSpeedInfo] {
        override def processElement(value: TrafficInfo, ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#ReadOnlyContext, out: Collector[OutOfLimitSpeedInfo]) = {
          //先从状态中得到当前卡口的限速信息
          val info: MonitorInfo = ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).get(value.monitorId)
          if(info!=null){ //表示当前这里车，经过的卡口是有限速的
            var limitSpeed= info.limitSpeed
            var realSpeed =value.speed
            if(limitSpeed * 1.1 < realSpeed){ //当前车辆超速通过卡口
              out.collect(new OutOfLimitSpeedInfo(value.car,value.monitorId,value.roadId,realSpeed,limitSpeed,value.actionTime))
            }
          }
        }

        override def processBroadcastElement(value: MonitorInfo, ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#Context, out: Collector[OutOfLimitSpeedInfo]) = {
          //把广播流中的数据保存到状态中
          ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).put(value.monitorId,value)
        }
      })
        .print()
//      .addSink(new WriteDataSink[OutOfLimitSpeedInfo](classOf[OutOfLimitSpeedInfo]))

    streamEnv.execute()

  }

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.streaming.api.scala._

    val sql = "select monitor_id,road_id,speed_limit,area_id from t_monitor_info where speed_limit > 0"
    val stream1: BroadcastStream[MonitorInfo] = env.addSource(new JdbcReadDataSource[MonitorInfo](classOf[MonitorInfo], sql))
      .broadcast(GlobalConstants.MONITOR_STATE_DESCRIPTOR)

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop52:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "msb004")
    val stream2: DataStream[TrafficInfo] = env.addSource(new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(), prop).setStartFromEarliest())
      .map(line => {
        var strs: Array[String] = line.split(",")
        //1600127946000,8228,55993,京K46744,39.6,58,31
        //时间，卡口，摄像头，车号，道路，区域
        new TrafficInfo(strs(0).toLong, strs(1), strs(2), strs(3), strs(4).toDouble, strs(5), strs(6))
      })

    val stream3: DataStream[OutOfLimitSpeedInfo] = stream2.connect(stream1).process(new BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo] {
      override def processElement(value: TrafficInfo, ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#ReadOnlyContext, out: Collector[OutOfLimitSpeedInfo]): Unit = {
        val info: MonitorInfo = ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).get(value.monitorId)
        if (info != null) {
          if (value.speed > (info.limitSpeed * 1.1)) {
            //            case class OutOfLimitSpeedInfo(car:String,monitorId:String,roadId:String,realSpeed:Double,limitSpeed:Int,actionTime:Long)
            out.collect(new OutOfLimitSpeedInfo(value.car, value.monitorId, value.roadId, value.speed, info.limitSpeed, value.actionTime))
          }
        }
      }

      override def processBroadcastElement(value: MonitorInfo, ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#Context, out: Collector[OutOfLimitSpeedInfo]): Unit = {
        ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).put(value.monitorId, value)
      }
    })
//      .print()

    val insertSql="INSERT INTO t_speeding_info(car,monitor_id,road_id,rea1_speed,limit_speed,action_time) values(?,?,?,?,?,?)"

    stream3.addSink(new JdbcWriteDataSource[OutOfLimitSpeedInfo](classOf[OutOfLimitSpeedInfo],insertSql))

    env.execute()
  }

}
