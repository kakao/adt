package com.kakao.adt.mysql.crawler;

import java.util.List;

import com.kakao.adt.mysql.metadata.Table;

public class MysqlCrawlData {

    private final Table table;
    private final List<List<Object>> rowList;
    
    public MysqlCrawlData(
            Table table, 
            List<List<Object>> rowList){
        this.table = table;
        this.rowList = rowList;
    }
    
    public Table getTable(){
        return this.table;
    }
    
    public List<Object> getRowAt(int i){
        return this.rowList.get(i);
    }
    
    public int getRowCount(){
        return this.rowList.size();
    }
    
    public List<List<Object>> getAllRows(){
        return this.rowList;
    }
    
}
