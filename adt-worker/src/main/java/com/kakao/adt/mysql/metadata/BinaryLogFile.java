package com.kakao.adt.mysql.metadata;

public class BinaryLogFile {

    private final String logName;
    private final long fileSize;
    
    public BinaryLogFile(String logName, long fileSize){
        this.logName = logName;
        this.fileSize = fileSize;
    }
    
    public String getLogName(){
        return this.logName;
    }
    
    public long getFileSize(){
        return this.fileSize;
    }
    
}
