package com.msb.monitor.constant

import org.apache.flink.api.common.state.MapStateDescriptor

//从kafka中读取的数据，车辆经过卡口的信息
//1600127946000,8228,55993,京K46744,39.6,58,31
//时间，卡口，摄像头，车号，道路，区域
case class TrafficInfo(actionTime:Long,monitorId:String,cameraId:String,car:String,speed:Double,roadId:String,areaId:String)
//卡口信息的样例类
case class MonitorInfo(monitorId:String,roadId:String,limitSpeed:Int,areaId:String)
//车辆超速的信息
case class OutOfLimitSpeedInfo(car:String,monitorId:String,roadId:String,realSpeed:Double,limitSpeed:Int,actionTime:Long)
//某个时间范围内卡口的平均车速和通过的车辆数量=>卡口堵车情况
case class AvgSpeedInfo(start:Long,end:Long,monitorId:String,avgSpeed:Double,carCount:Int)
//套牌车辆告警信息对象
case class RepetitionCarWarning(car:String,firstMonitor:String,secondMonitor:String,msg:String,actionTime:Long)
//危险驾驶的信息
case class DangerousDrivingWarning(car:String,msg:String,createTime:Long,avgSpeed:Double)
//违法车辆信息对象
case class ViolationInfo(car:String,msg:String,createTime:Long)
//车辆轨迹数据样例类
case class TrackInfo(car:String,actionTime:Long,monitorId:String,roadId:String,speed:Double)


object GlobalConstants {
  lazy val MONITOR_STATE_DESCRIPTOR=new MapStateDescriptor[String,MonitorInfo]("monitor_info",classOf[String],classOf[MonitorInfo])
}
