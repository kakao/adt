package com.kakao.adt.parallel;

public interface IParallelExecutor <I, O> {

    public boolean input(I input);
    public O process(I input);
    public void completeHandler(O output);
    public void onException(Exception e);
    
}
