package com.kakao.adt.mysql.metadata;

import java.util.*;

public class Table {

    private Database database;
    
    private String name;
    private List<Column> columns = new ArrayList<>();
    private List<Index> indexes = new ArrayList<>();
    
    public Database getDatabase() {
        return database;
    }
    
    public void setDatabase(Database database) {
        this.database = database;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void addColumn(Column col){
        this.columns.add(col);
    }
    
    public Column getColumn(String colName){
        for(final Column col : columns){
            if(col.getName().equals(colName)){
                return col;
            }
        }
        throw new IllegalArgumentException("unknown column name [" + colName + "]");
    }
    
    public List<Column> getColumns(){
        return this.columns;
    }
    
    public void addIndex(Index idx){
        this.indexes.add(idx);
    }
    
    public int getIndexListSize(){
        return indexes.size();
    }
    
    public List<Index> getIndexes(){
        return this.indexes;
    }
    
    public Index getIndex(int i){
        return indexes.get(i);
    }
    
}
