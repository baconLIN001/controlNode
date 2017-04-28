namespace java com.bacon.controlNode

enum RequestType {
    JOB,
    DATA
}


struct WebRequest {
    1: required RequestType type;  // 请求的类型，必选
    2: required i32 taskId;     //请求的task的ID
    3: required string param;       // 请求传参
    4: required string action;      //请求命令
    5: optional string message;     // 请求说明
}

exception RequestException {
    1: required i32 code;
    2: optional string reason;
}

// 服务名
service BaseService {
    string doJobAction(1: WebRequest webRequest) throws (1:RequestException qe); // 可能抛出异常。
    string doDataAction(1: WebRequest webRequest) throws(1:RequestException qe);
}

