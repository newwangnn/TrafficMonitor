package com.msb.monitor.analysis.trafficinterface.process;

import javax.servlet.http.HttpServletRequest;

/**
 * 数据采集服务器中的业务接口
 */
public interface DataService {

    void process(String dataType, HttpServletRequest request) throws  Exception;
}
