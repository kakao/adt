package com.kakao.adt;

import com.kakao.adt.handler.ProcessorHandler;

public interface IProcessor<T> {

    void start() throws Exception;
    void stop();
    void waitStop() throws Exception;
    void processData(T task) throws Exception;
    ProcessorHandler<T> getHandler();
    
}
