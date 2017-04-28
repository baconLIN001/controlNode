package com.bacon.controlNode.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

/**
 * Created by Lee on 2016/11/10 0010.
 *
 * VersionCode:1.1
 * 主要优化了，connector的flush.size
 */


public class ConnectorUtils {

    static Logger logger = Logger.getLogger(ConnectorUtils.class);

    private static final String SERVLET_DELETE = "DELETE";





    //    获取所有connectors的信息
    public static String getAllConnectors(String serverIp) {
        String requestUrl = "http://" + serverIp + ":8083/connectors";
        return doGet(requestUrl, null);
    }

    //    获取单个connector的信息
    public static String getAConnectors(String serverIp, String topic) {
        String requestUrl = "http://" + serverIp + ":8083/connectors/" + topic+"-sink";
        return doGet(requestUrl, null);
    }

    //新建一个Connector
    public static String createNewConncetors(String serverIp, String topic,String namenode_ip,int argNum) {
        String requestUrl = "http://" + serverIp + ":8083/connectors";

        return Integer.toString(doPostJson(requestUrl, 10000, topic,namenode_ip,argNum));
    }

    //    删除一个connector
    public static String deleteConnectors(String serverIp, String topic) {
        String requestUrl = "http://" + serverIp + ":8083/connectors/"+topic+"-sink";
        return Integer.toString(doDelete(requestUrl, null));
    }

    /**
     * 根据URL创建一个GetMethod，然后设置一些参数返回
     *
     * @param url     需要抓取的网站URL
     * @param timeout 超时
     * @return 返回得到的GetMethod
     */
    public static int doPostJson(String url, int timeout, String topic,String namenode_ip,int argNum) {

        int statusCode = 404;
        PostMethod method = null;
        long flushSize;
        if(argNum<=1){
            flushSize=1000000;
        }
        else {
            /*
            * 计算公式说明：
            * 由已知  1一个域  100000行的 avro数据的  占用空间约为1.6MB
            * 固有  argNum*1.6*x<128
            * 其中x为  x个100000行。既生成的文件要小于128MB，由于1.6是一个估计值，所以公式修改为
            * argNum*1.6*x<115（理论上如果不出现过长的域，则不会产生超过128MB的avro文件）
            * x=120/(1.6*argNum)
            *
            * */
            flushSize=(72/argNum)*100000;
        }

        try {
            method = new PostMethod(url);
            JSONObject jsonObject = new JSONObject();
            JSONObject jsonArray = new JSONObject();


            jsonObject.put("connector.class", "io.confluent.connect.hdfs.HdfsSinkConnector");
            jsonObject.put("flush.size", Long.toString(flushSize));
            jsonObject.put("tasks.max", "1");
            jsonObject.put("topics", topic);
            jsonObject.put("shutdown.timeout.ms", "3000");
            jsonObject.put("name", topic + "-sink");
            jsonObject.put("hdfs.url", "hdfs://"+namenode_ip+":9000");
            jsonObject.put("rotate.interval.ms", 60000);
            jsonArray.put("name", topic + "-sink");
            jsonArray.put("config", jsonObject);

//            jsonObject.put()

            String transJson = jsonArray.toString();
            System.out.println(jsonArray.toString());

            RequestEntity se = new StringRequestEntity(transJson, "application/json", "UTF-8");
            method.setRequestEntity(se);
            //使用系统提供的默认的恢复策略
            method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            //设置超时的时间
            method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);

            HttpClient httpClient = new HttpClient();
            statusCode = httpClient.executeMethod(method);
        } catch (IllegalArgumentException e) {
            logger.error("非法的URL：{}" + url);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (HttpException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return statusCode;
    }


    public static String doGet(String url, Map<String, Object> paramMap) {
        if (paramMap != null) {
            String paramStr = prepareParam(paramMap);
            if (paramStr == null || paramStr.trim().length() < 1) {

            } else {
                url += "?" + paramStr;
            }
        }
        String result = "";
        BufferedReader in = null;
        try {
//            String urlNameString = url + "?" + param;
//            URL realUrl = new URL(urlNameString);
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 建立实际的连接
            connection.connect();
            // 获取所有响应头字段
            Map<String, List<String>> map = connection.getHeaderFields();
            // 遍历所有的响应头字段
            for (String key : map.keySet()) {
                System.out.println(key + "--->" + map.get(key));
            }
            // 定义 BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输入流
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }


    public static int doDelete(String urlStr, Map<String, Object> paramMap) {
        if (paramMap != null) {
            String paramStr = prepareParam(paramMap);
            if (paramStr == null || paramStr.trim().length() < 1) {

            } else {
                urlStr += "?" + paramStr;
            }
        }
        System.out.println(urlStr);
        URL url = null;
        try {
            url = new URL(urlStr);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod(SERVLET_DELETE);
            //屏蔽掉的代码是错误的，java.net.ProtocolException: HTTP method DELETE doesn't support output
/*		OutputStream os = conn.getOutputStream();
        os.write(paramStr.toString().getBytes("utf-8"));
		os.close();  */
            if (conn.getResponseCode() == 200) {
                return 0;
            } else {
                return conn.getResponseCode();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 404;
        }
    }

    private static String prepareParam(Map<String, Object> paramMap) {
        StringBuffer sb = new StringBuffer();
        if (paramMap.isEmpty()) {
            return "";
        } else {
            for (String key : paramMap.keySet()) {
                String value = (String) paramMap.get(key);
                if (sb.length() < 1) {
                    sb.append(key).append("=").append(value);
                } else {
                    sb.append("&").append(key).append("=").append(value);
                }
            }
            return sb.toString();
        }
    }
}
