import com.alibaba.fastjson.JSON;
import com.bacon.controlNode.client.DataManagerClient;
import com.bacon.controlNode.entity.Action;
import com.bacon.controlNode.entity.Parameter;
import com.bacon.controlNode.entity.RequestType;
import com.bacon.controlNode.entity.WebRequest;

/**
 * Created by bacon on 2017/4/14.
 */
public class App {
    public static void main(String[] args){
        DataManagerClient.ServerConfig config = new DataManagerClient.ServerConfig("127.0.0.1",9932,1000);
        DataManagerClient client = DataManagerClient.INSTANCE;
        client.setServerConfig(config);
        client.openClient();
        WebRequest request = new WebRequest();
        Parameter parameter = new Parameter();
        parameter.setTopic("test2");
        parameter.setFieldNum(3);
        request.setType(RequestType.DATA).setParam(JSON.toJSONString(parameter)).setAction(Action.FILE_PERSISTENCE).setTaskId(5);
        String response = client.getData(JSON.toJSONString(request));
        System.out.println(response);
        client.closeClient();
    }
}
