package com.kakao.adt.mysql.metadata;

import java.util.*;

public class Database {

    private String name;
    private Map<String, Table> tables = new HashMap<>(); 
    
    public String getName(){
        return name;
    }
    
    public void setName(String name){
        this.name = name;
    }
    
    public void addTable(Table table){
        this.tables.put(table.getName(), table);
    }
    
    public Table getTable(String tableName){
        return this.tables.get(tableName);
    }
    
    public Collection<Table> getTables(){
        return this.tables.values();
    }
    
}
