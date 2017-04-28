package com.bacon.controlNode.server;

import com.bacon.controlNode.service.BaseService;
import com.bacon.controlNode.service.DataCSevice;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DataManagerServer {
    Logger log = LoggerFactory.getLogger(DataManagerServer.class);

    //非阻塞server
    public void startServer(){
        InputStream in = DataManagerServer.class.getResourceAsStream("../../../../setting.properties");
        Properties props = new Properties();
        try {
            props.load(in);

            int port = Integer.parseInt(props.getProperty("Job_Manager.server.port"));
            TProcessor processor = new BaseService.Processor(new DataCSevice());
            TNonblockingServerSocket tNonblockingServerSocket = null;

            tNonblockingServerSocket = new TNonblockingServerSocket(port);

            TNonblockingServer.Args tnbArgs = new TNonblockingServer.Args(tNonblockingServerSocket);
            tnbArgs.processor(processor);
            tnbArgs.transportFactory(new TFramedTransport.Factory());
            tnbArgs.protocolFactory(new TCompactProtocol.Factory());
            //使用非阻塞式IO，服务端和客户端需要指定TFramedTransport数据传输的方式
            TServer server = new TNonblockingServer(tnbArgs);
            log.info("Data Server is running ...");
            server.serve();

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
