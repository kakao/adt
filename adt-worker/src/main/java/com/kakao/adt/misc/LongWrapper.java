package com.kakao.adt.misc;

public class LongWrapper {

    private volatile long value = 0;
    
    public void setValue(long value){
        this.value = value;
    }
    
    public long getValue(){
        return value;
    }
    
    public long add(long value){
        this.value += value;
        return this.value;
    }
    
}
