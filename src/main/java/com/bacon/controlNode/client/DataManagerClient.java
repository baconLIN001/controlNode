package com.bacon.controlNode.client;


import com.alibaba.fastjson.JSON;
import com.bacon.controlNode.entity.Action;
import com.bacon.controlNode.entity.RequestType;
import com.bacon.controlNode.entity.WebRequest;
import com.bacon.controlNode.service.BaseService;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public enum DataManagerClient {
    INSTANCE;
    //服务端配置
    private ServerConfig serverConfig;

    Logger log = LoggerFactory.getLogger(DataManagerClient.class);
    private TTransport transport = null;

    private BaseService.Client client;
    private BaseService.AsyncClient asyncClient;

    private DataManagerClient() {

    }

    //获取同步client
    public void openClient() {
        try {
            transport = new TFramedTransport(new TSocket(serverConfig.serverHost, serverConfig.port, serverConfig.timeout));
            //以帧的形式发送，每帧前面是一个长度。要求服务器是non-blocking server
            TProtocol protocol = new TCompactProtocol(transport);
            client = new BaseService.Client(protocol);

            log.info("data manager client start...");
            transport.open();

        } catch (TTransportException e) {
            log.info("data manager client start error " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 异步
     *
     * @throws java.net.ConnectException
     */
    public void openAsyncClient() {
        try {
            TAsyncClientManager clientManager = new TAsyncClientManager();
            transport = new TNonblockingSocket(serverConfig.serverHost, serverConfig.port, serverConfig.timeout);
            TProtocolFactory tProtocolFactory = new TCompactProtocol.Factory();
            asyncClient = new BaseService.AsyncClient(tProtocolFactory, clientManager, (TNonblockingTransport) transport);

            log.info("data manager async client start...");
        } catch (IOException e) {
            log.info("data manager async client start error " + e.getMessage());
            e.printStackTrace();
        }


    }

    public void closeClient() {
        if (transport != null) {
            transport.close();
        }
    }

    //向服务器发送请求
    private String call(WebRequest request) {
        String response = null;
        try {
            response = client.doDataAction(request);
        } catch (TException e) {
            e.printStackTrace();
        }
        return response;
    }

    public String getData(String webRequest){
        WebRequest request = JSON.parseObject(webRequest,WebRequest.class);
        return call(request);
    }



    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }


    public static class ServerConfig {
        private String serverHost;
        private int port;
        private int timeout;

        public ServerConfig(String serverHost, int port) {
            this.serverHost = serverHost;
            this.port = port;
        }

        public ServerConfig(String serverHost, int port, int timeout) {
            this.serverHost = serverHost;
            this.port = port;
            this.timeout = timeout;
        }

        public String getServerHost() {
            return serverHost;
        }

        public void setServerHost(String serverHost) {
            this.serverHost = serverHost;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getTimeout() {
            return timeout;
        }

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }
    }
}
