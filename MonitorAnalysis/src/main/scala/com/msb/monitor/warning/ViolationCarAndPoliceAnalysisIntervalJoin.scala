package com.msb.monitor.warning

import com.msb.monitor.constant.{PoliceAction, ViolationInfo}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
  * 违法车辆和交警出警分析
  * 第一种，当前的违法车辆（在5分钟内）如果已经出警了。（最后输出道主流中做删除处理）。
  * 第二种，当前违法车辆（在5分钟后）交警没有出警（发出出警的提示，在侧流中发出）。
  * 第三种，有交警的出警记录，但是不是由监控平台报的警。
  * 需要两种数据流：
  * 1、系统的实时违法车辆的数据流
  * 2、交警实时出警记录数据
  */
object ViolationCarAndPoliceAnalysisIntervalJoin {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

//    交警出警记录
    val stream1: DataStream[PoliceAction] = env.socketTextStream("hadoop52", 9999)
      .map(line => {
        val strs: Array[String] = line.split(",")
        //          6389,皖M16450,罚款200元,1600165650000
        new PoliceAction(strs(0), strs(1), strs(2), strs(3).toLong)
      })
      .assignAscendingTimestamps(_.actionTime)
//违章车辆
    val stream2: DataStream[ViolationInfo] = env.socketTextStream("hadoop52", 8888)
      .map(line => {
        val strs: Array[String] = line.split(",")
        new ViolationInfo(strs(0), strs(1), strs(2).toLong)
      })
      .assignAscendingTimestamps(_.createTime)


    stream1.keyBy(_.car)
        .intervalJoin(stream2.keyBy(_.car))
        .between(Time.seconds(-5),Time.seconds(5))
        .process(new ProcessJoinFunction[PoliceAction,ViolationInfo,String]() {
          override def processElement(left: PoliceAction, right: ViolationInfo, ctx: ProcessJoinFunction[PoliceAction, ViolationInfo, String]#Context, out: Collector[String]): Unit = {
            out.collect(s"出警警员id:${left.policeId},违规车辆：${left.car},处理结果：${left.actionStatus}")
          }
        })
        .print()

    env.execute()

    //需要两个数据流的连接，而且这两个中数据流有连接条件：PoliceAction.car =ViolationInfo.car
    //这个连接相当于数据库中的内连接
//    stream1.keyBy(_.car)
//      .intervalJoin(stream2.keyBy(_.car)) //内连接 intervalJoin只能应用在keyedStream，普通的DataStream流连接只能使用connect
//      .between(Time.seconds(-5),Time.seconds(5)) //设置一个时间边界，在这个边界中，两个流的数据自动关联,为了便于测试，使用5秒替代5分钟 ,不和Watermark的延迟有关系
//      .process(new ProcessJoinFunction[ViolationInfo,PoliceAction,String] {
//      //在时间边界内，存在车辆号码相同的两中数据，
//      override def processElement(left: ViolationInfo, right: PoliceAction, ctx: ProcessJoinFunction[ViolationInfo, PoliceAction, String]#Context, out: Collector[String]) = {
//        out.collect(s"车辆${left.car},已经有交警出警了，警号为:${right.policeId},出警的状态是：${right.actionStatus},出警的时间:${right.actionTime}")
//      }
//    })
//      .print()

  }


}
