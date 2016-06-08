package com.kakao.adt.misc;

/**
 * thread-unsafe
 * @author gordon
 *
 */

public class BooleanWrapper {

    volatile private boolean value;
    
    public BooleanWrapper(boolean initValue){
        this.value = value;
    }
    
    public boolean get(){
        return value;
    }
    
    public void set(boolean v){
        this.value = v;
    }
    
    public boolean compareAndSet(boolean expected, boolean newValue){
        if(this.value != expected){
            return false;
        }
        
        this.value = newValue;
        return true;
    }
    
}
