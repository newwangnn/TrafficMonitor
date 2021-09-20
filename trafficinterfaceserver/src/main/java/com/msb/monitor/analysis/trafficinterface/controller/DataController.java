package com.msb.monitor.analysis.trafficinterface.controller;

import com.msb.monitor.analysis.trafficinterface.bean.ResponseCodePropertyConfig;
import com.msb.monitor.analysis.trafficinterface.constant.ResponseConstant;
import com.msb.monitor.analysis.trafficinterface.process.DataService;
import com.msb.monitor.analysis.trafficinterface.bean.ResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
/**
 * 数据采集服务器的控制器
 * http://localhost:8686/controller/sendData/carInfo
 * 数据的具体内容通过请求头传入
 */
@RestController
@RequestMapping("/controller")
public class DataController {

    @Autowired
    private ResponseCodePropertyConfig config;

    @Autowired
    private DataService service;


    @PostMapping("/sendData/{dataType}")
    public Object collect(@PathVariable("dataType") String dataType , HttpServletRequest request ) throws  Exception{
        service.process(dataType,request);
        return new ResponseEntity(ResponseConstant.CODE_0000,config.getMsg(ResponseConstant.CODE_0000),dataType);
    }


}
