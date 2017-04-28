package com.bacon.controlNode.entity;

/**
 * Created by qiaqia on 2017/4/13.
 */
public class Action {
    //任务管理模块请求命令
    public final static String SUBMIT_SPARK_JOB = "submit_spark_job";
    public final static String SUBMIT_JAVA_JOB ="submit_java_job";
    public final static String SUBMIT_MR_JOB = "submit_mr_job";
    public final static String QUERY_JOB_INFO = "query_job_info";
    public final static String QUERY_JOB_LOG = "query_job_lob";
    public final static String QUERY_JOB_ERROR_LOG = "query_job_error_job";
    public final static String QUERY_JOB_DEFINITION = "query_job_definition";
    public final static String IS_ALIVE ="is_alive";

    //数据接入模块请求命令
    public final static String GET_DATA = "get_data";
    public final static String FILE_PERSISTENCE = "data_persistence";


}
