package com.msb.monitor.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.msb.monitor.constant.{AvgSpeedInfo, MonitorInfo, OutOfLimitSpeedInfo, RepetitionCarWarning}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class JdbcWriteDataSource[T](classType: Class[_ <: T], sql: String) extends RichSinkFunction[T] {

  var connection: Connection = _;
  var ps: PreparedStatement = _;


  override def open(parameters: Configuration): Unit = {

    connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/trafficmonitor?serverTimezone=UTC&useSSL=false", "root", "root")
    ps = connection.prepareStatement(sql)

  }

  override def close(): Unit = {
    ps.close()
    connection.close()
  }

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {

    if (classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)) {
      //         val insertSql="insert into t_speeding_info(car,monitor_id,road_id,real_speed,limit_speed,action_time) values(?,?,?,?,?,?)"
      val info: OutOfLimitSpeedInfo = value.asInstanceOf[OutOfLimitSpeedInfo]
      ps.setString(1, info.car)
      ps.setString(2, info.monitorId)
      ps.setString(3, info.roadId)
      ps.setDouble(4, info.realSpeed)
      ps.setInt(5, info.limitSpeed)
      ps.setLong(6, info.actionTime)
      ps.executeUpdate()

    }

    if (classType.getName.equals(classOf[AvgSpeedInfo].getName)) {
      //      insert into t_average_speed(start_time,end_time,monitor_id,avg_speed,car_count ) values(?,?,?,?,?)
      val info: AvgSpeedInfo = value.asInstanceOf[AvgSpeedInfo]
      ps.setLong(1, info.start)
      ps.setLong(2, info.end)
      ps.setString(3, info.monitorId)
      ps.setDouble(4, info.avgSpeed)
      ps.setInt(5, info.carCount)
      ps.executeUpdate()
    }

    if (classType.getName.equals(classOf[RepetitionCarWarning].getName)) {
      val info: RepetitionCarWarning = value.asInstanceOf[RepetitionCarWarning]
      val timestamp: Long = info.actionTime

      val format = new SimpleDateFormat("yyyy-MM-dd")
      val date: Date = format.parse(format.format(new Date(timestamp)))

      val pstemp: PreparedStatement = connection.prepareStatement("SELECT count(1) FROM t_repeat_car WHERE car=? AND create_time BETWEEN ? AND ? ")
      pstemp.setString(1, info.car)
      pstemp.setLong(2, date.getTime)
      pstemp.setLong(3, date.getTime+24*60*60*1000)
      val result: ResultSet = pstemp.executeQuery()
      if(result.next()&&result.getInt(1)==0){
        //       val sql = "INSERT INTO t_repeat_car(car,first_monitor_id,second_monitor_id,msg,create_time) VALUES(?,?,?,?,?)"
        ps.setString(1,info.car)
        ps.setString(2,info.firstMonitor)
        ps.setString(3,info.secondMonitor)
        ps.setString(4,info.msg)
        ps.setLong(5,info.actionTime)
        ps.executeUpdate()

      }
      result.close()
      pstemp.close()

    }
  }
}
