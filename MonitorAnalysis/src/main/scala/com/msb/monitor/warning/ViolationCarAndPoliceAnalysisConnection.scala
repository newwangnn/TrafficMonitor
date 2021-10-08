package com.msb.monitor.warning

import com.msb.monitor.constant.{PoliceAction, ViolationInfo}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ViolationCarAndPoliceAnalysisConnection {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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
//        皖M16450,违章超速12次,1600165750000
        new ViolationInfo(strs(0), strs(1), strs(2).toLong)
      })
      .assignAscendingTimestamps(_.createTime)

    val pTag = new OutputTag[PoliceAction]("p")
    val vTag = new OutputTag[ViolationInfo]("v")

    val stream3: DataStream[String] = stream1.keyBy(_.car).connect(stream2.keyBy(_.car))
      .process(new KeyedCoProcessFunction[String, PoliceAction, ViolationInfo, String] {

        lazy val ps: ValueState[PoliceAction] = getRuntimeContext.getState[PoliceAction](new ValueStateDescriptor[PoliceAction]("p", classOf[PoliceAction]))
        lazy val vs: ValueState[ViolationInfo] = getRuntimeContext.getState[ViolationInfo](new ValueStateDescriptor[ViolationInfo]("v", classOf[ViolationInfo]))

        override def processElement1(value: PoliceAction, ctx: KeyedCoProcessFunction[String, PoliceAction, ViolationInfo, String]#Context, out: Collector[String]): Unit = {

          //            交警记录出现时，查看违章车辆是否存在，若不存在说明有出警但没有违章车辆，若存在说明已经处理过违章车辆同时删除时间触发器
          val violation: ViolationInfo = vs.value()
          if (violation == null) {
            ctx.timerService().registerEventTimeTimer(value.actionTime + 5000)
            ps.update(value)
          } else {
            out.collect(s"违法车辆${value.car}，违法情况${violation.msg},已经有交警出警了，警号为:${value.policeId},出警的状态是：${value.actionStatus}")
            ps.clear()
            vs.clear()
          }

        }

        override def processElement2(value: ViolationInfo, ctx: KeyedCoProcessFunction[String, PoliceAction, ViolationInfo, String]#Context, out: Collector[String]): Unit = {

          val police: PoliceAction = ps.value()
          if (police == null) {
            ctx.timerService().registerEventTimeTimer(value.createTime + 5000)
            vs.update(value)
          } else {
            out.collect(s"违法车辆${value.car}，违法时间${police.actionTime},已经有交警出警了，警号为:${police.policeId},出警的状态是：${police.actionStatus}")
            ps.clear()
            vs.clear()
          }

        }

        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, PoliceAction, ViolationInfo, String]#OnTimerContext, out: Collector[String]): Unit = {

          //            5秒内没有交警出警或违章车辆对应出现
          val police: PoliceAction = ps.value()
          val violation: ViolationInfo = vs.value()

          if (police == null && violation != null) {
            ctx.output(vTag, violation)
          }
          if (police != null && violation == null) {
            //表示有出警记录，但是没有匹配的违法车辆
            ctx.output(pTag, police)
          }

          ps.clear()
          vs.clear()

        }
      })
    stream3.print("出警+违章")
    stream3.getSideOutput(pTag).print("出警记录")
    stream3.getSideOutput(vTag).print("违章车辆")



    env.execute()


//    val tag1 = new OutputTag[PoliceAction]("No Violaction Car!")
//    val tag2 = new OutputTag[ViolationInfo]("No PoliceAction!")
//
//    var mainStream=stream1.keyBy(_.car).connect(stream2.keyBy(_.car))
//      .process(new KeyedCoProcessFunction[String,ViolationInfo,PoliceAction,String] {
//
//        //需要两个状态，分别保存违法数据，出警记录
//        lazy val vState: ValueState[ViolationInfo] = getRuntimeContext.getState(new ValueStateDescriptor[ViolationInfo]("v",classOf[ViolationInfo]))
//        lazy val pState: ValueState[PoliceAction] = getRuntimeContext.getState(new ValueStateDescriptor[PoliceAction]("p",classOf[PoliceAction]))
//
//        override def processElement1(value: ViolationInfo, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#Context, out: Collector[String]) = {
//          val policeAction:PoliceAction = pState.value()
//          if(policeAction==null){//可能出警的数据还没有读到，或者该违法处理还没有交警出警
//            ctx.timerService().registerEventTimeTimer(value.createTime+5000) //5秒后触发提示
//            vState.update(value)
//          }else{ //已经有一条与之对应的出警记录,可以关联
//            out.collect(s"该违法车辆${value.car}，违法时间${value.createTime},已经有交警出警了，警号为:${policeAction.policeId},出警的状态是：${policeAction.actionStatus},出警的时间:${policeAction.actionTime}")
//            vState.clear()
//            pState.clear()
//          }
//        }
//
//        //当从第二个流中读取一条出警记录数据
//        override def processElement2(value: PoliceAction, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#Context, out: Collector[String]) = {
//          val info: ViolationInfo = vState.value()
//          if(info==null){ //出警记录没有找到对应的违法车辆信息
//            ctx.timerService().registerEventTimeTimer(value.actionTime+5000)
//            pState.update(value)
//          }else{//已经有一条与之对应的出警记录,可以关联
//            out.collect(s"该违法车辆${info.car}，违法时间${info.createTime},已经有交警出警了，警号为:${value.policeId},出警的状态是：${value.actionStatus},出警的时间:${value.actionTime}")
//            vState.clear()
//            pState.clear()
//          }
//        }
//
//        //触发器触发的函数
//        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#OnTimerContext, out: Collector[String]) = {
//          val info: ViolationInfo = vState.value()
//          val action: PoliceAction = pState.value()
//          if(info==null&& action!=null){//表示有出警记录，但是没有匹配的违法车辆
//            ctx.output(tag1,action)
//          }
//          if(action==null&& info!=null){ //有违法车辆信息，但是5分钟内还没有出警记录
//            ctx.output(tag2,info)
//          }
//          //清空状态
//          pState.clear()
//          vState.clear()
//        }
//      })
//
//    mainStream.print()
//    mainStream.getSideOutput(tag1).print("没有对应的违法车辆信息")
//    mainStream.getSideOutput(tag2).print("该违法车辆在5分钟内没有交警出警")


  }

}
