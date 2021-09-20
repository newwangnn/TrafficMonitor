package com.msb.monitor.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.msb.monitor.constant.{MonitorInfo, TrafficInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class JdbcReadDataSource[T](classType:Class[_<:T],sql:String) extends RichSourceFunction[T]{
  var flag:Boolean = true;
  var connection: Connection = _;
  var ps: PreparedStatement = _;
  var rs: ResultSet = _;

  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection("jdbc:mysql://172.17.1.141:3306/trafficmonitor?serverTimezone=UTC&useSSL=false","root","root")
    ps = connection.prepareStatement(sql)
  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    while (flag){
      rs = ps.executeQuery()
      while (rs.next()){
        if(classType.getName.equals(classOf[MonitorInfo].getName)){
          var info =new MonitorInfo(rs.getString(1),rs.getString(2),rs.getInt(3),rs.getString(4))
          ctx.collect(info.asInstanceOf[T])
        }

      }
      rs.close()
    }

  }


  override def cancel(): Unit = {
      flag=false
  }

  override def close(): Unit = {
    ps.close()
    connection.close()
  }
}
