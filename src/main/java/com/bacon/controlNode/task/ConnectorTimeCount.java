package com.bacon.controlNode.task;

import com.bacon.controlNode.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bacon on 2017/4/17.
 */
public class ConnectorTimeCount implements Runnable {

    Logger logger = LoggerFactory.getLogger(ConnectorTimeCount.class);
    long connectorRunTime = 12*60*60;

    private String topic;
    private String serverIp;

    public ConnectorTimeCount(){}
    public ConnectorTimeCount(String serverIp, String topic){
        this.topic=topic;
        this.serverIp=serverIp;
    }
    @Override
    public void run() {
        while (connectorRunTime>0){
            connectorRunTime--;
            try {
                Thread.sleep(1000);
//                System.out.println("还剩:"+connectorRunTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        System.out.println("完成");
        ConnectorUtils.deleteConnectors(serverIp,topic);
        logger.info("close the connector: server ip = " + serverIp + ", topic = " + topic);
    }
}
