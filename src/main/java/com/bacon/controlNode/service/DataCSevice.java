package com.bacon.controlNode.service;

import com.alibaba.fastjson.JSON;
import com.bacon.controlNode.entity.Action;
import com.bacon.controlNode.entity.Parameter;
import com.bacon.controlNode.entity.RequestType;
import com.bacon.controlNode.entity.WebRequest;
import com.bacon.controlNode.exception.RequestException;
import com.bacon.controlNode.task.ConnectorTimeCount;
import com.bacon.controlNode.util.AsyncTaskUtils;
import com.bacon.controlNode.util.ConnectorUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Created by bacon on 2017/4/14.
 */
public class DataCSevice extends BaseServiceImpl {

    Logger logger = LoggerFactory.getLogger(DataCSevice.class);

    @Override
    public String doDataAction(WebRequest webRequest) throws RequestException, TException{
        if (webRequest.getType().equals(RequestType.DATA)){
            logger.info("控制节点接收数据接入请求：" + webRequest);

            if (webRequest.getAction().equals(Action.FILE_PERSISTENCE)){
                logger.info("请求类型：数据持久化");

                Properties properties = new Properties();
                try {
                    InputStream inputStream = new BufferedInputStream(new FileInputStream("server.properties"));
                    properties.load(inputStream);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String server_ip = properties.getProperty("connector_server_ip");
                String namenode_ip = properties.getProperty("namenode_ip");

                Parameter parameter = JSON.parseObject(webRequest.getParam(),Parameter.class);
                String topic = parameter.getTopic();

                ConnectorUtils.createNewConncetors(server_ip,topic,namenode_ip,parameter.getFieldNum());
                //计时半天后关闭connector
                ConnectorTimeCount connectorTimeCount = new ConnectorTimeCount(server_ip,topic);
                AsyncTaskUtils.INSTANCE.dispatchNormalTask(connectorTimeCount);
            }
        }else {
            return "非数据接入请求";
        }
        return "SUCCESS";
    }
}
