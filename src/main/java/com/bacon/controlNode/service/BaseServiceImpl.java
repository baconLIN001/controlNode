package com.bacon.controlNode.service;

import com.bacon.controlNode.entity.WebRequest;
import com.bacon.controlNode.exception.RequestException;
import org.apache.thrift.TException;

/**
 * Created by qiaqia on 2017/4/13.
 */
public class BaseServiceImpl implements BaseService.Iface {

    public String doJobAction(WebRequest webRequest) throws RequestException, TException {
        return "abc";
    }

    public String doDataAction(WebRequest webRequest) throws RequestException, TException {
        return "";
    }



}
