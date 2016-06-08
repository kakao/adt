package com.kakao.adt.mysql.crawler;

public enum MysqlSelectLockMode {
    None(""),
    ForUpdate(" FOR UPDATE "),
    LockInShareMode(" LOCK IN SHARE MODE "),
    ;
 
    private final String sql;
    private MysqlSelectLockMode(String sql){
        this.sql = sql;
    }
    public String getSql(){
        return this.sql;
    }
    
}
