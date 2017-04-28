import com.bacon.controlNode.task.ConnectorTimeCount;
import com.bacon.controlNode.util.AsyncTaskUtils;

/**
 * Created by bacon on 2017/4/17.
 */
public class ConnectorTimeCountTest {
    public static void main(String[] args){
        ConnectorTimeCount connectorTimeCount = new ConnectorTimeCount();
        AsyncTaskUtils.INSTANCE.dispatchNormalTask(connectorTimeCount);
    }
}
