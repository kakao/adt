package com.kakao.adt.handler;

public interface ProcessorHandler<T> {

    void beforeStart(); //not supported yet...
    void afterStop(); //not supported yet...
    void onException(Exception e); //not supported yet...
    void processData(T data) throws Exception;
    
}
