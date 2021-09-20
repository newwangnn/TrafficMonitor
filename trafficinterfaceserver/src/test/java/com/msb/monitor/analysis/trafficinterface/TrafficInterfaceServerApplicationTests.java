package com.msb.monitor.analysis.trafficinterface;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class TrafficInterfaceServerApplicationTests {

	/*

	        try{

            int i =0;
            while (i<20){
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type","application/json");
                String data ="11,22,33,京P12345,57.2,"+i;
                post.setEntity(new StringEntity(data, Charset.forName("UTF-8")));
                HttpResponse response =client.execute(post); //发送数据
                i++;
                Thread.sleep(1000);

                //响应的状态如果是200的话，获取服务器返回的内容
                if(response.getStatusLine().getStatusCode()== HttpStatus.SC_OK){
                    result=EntityUtils.toString(response.getEntity(),"UTF-8");
                }
                System.out.println(result);
            }

	*/

	@Test
	void contextLoads() {

		CloseableHttpResponse response=null;

		try {
			CloseableHttpClient httpclient = HttpClients.createDefault();

			int i =0;
			while (i<20) {
				HttpPost httpPost = new HttpPost("http://localhost:8686/controller/sendData/traffic_data");
				httpPost.setHeader("Content-Type", "application/json");

				String data = "11,22,33,京P12345,57.2," + i;
				httpPost.setEntity(new StringEntity(data, Charset.forName("UTF-8")));

				response = httpclient.execute(httpPost); //发送数据
				i++;
				Thread.sleep(1000);

				//响应的状态如果是200的话，获取服务器返回的内容
				if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
					String result = EntityUtils.toString(response.getEntity(), "UTF-8");
					System.out.println(result);
				}

			}
		}catch (Exception e) {

		}finally {
			try {
				if(response!=null)
				response.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

}
