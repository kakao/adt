package com.kakao.adt.mysql.metadata;

public class Column {

    private Table table;
    
    private String name;
    private int position;
    private String dataType;
    
    public Table getTable() {
        return table;
    }
    
    public void setTable(Table table) {
        this.table = table;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * start from 0
     * @return
     */
    public int getPosition(){
        return this.position;
    }
    
    /**
     * start from 0
     * @return
     */
    public void setPosition(int position){
        this.position = position;
    }
    
    public String getDataType() {
        return dataType;
    }
    
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}
