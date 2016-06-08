package com.kakao.adt.mysql.metadata;

import java.util.ArrayList;
import java.util.List;

public class Index {

    private Table table;
    
    private String name;
    private boolean isUnique;
    private List<Column> columns = new ArrayList<>();

    public Table getTable(){
        return table;
    }
    
    public void setTable(Table table){
        this.table = table;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public boolean isUnique() {
        return isUnique;
    }
    
    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }
    
    public void addColumn(Column col){
        this.columns.add(col);
    }
    
    public List<Column> getColumns(){
        return this.columns;
    }
    
    public Column getColumn(int i){
        return this.columns.get(i);
    }
    
}
