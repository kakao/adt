package com.kakao.adt.mysql.binlog;

import java.util.Collections;
import java.util.List;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.kakao.adt.mysql.metadata.Table;

public class MysqlBinlogData {

    private final Table table;
    private final MysqlBinlogHandler binlogType;
    private final List<? extends Column> rowBefore;
    private final List<? extends Column> rowAfter;
    
    public MysqlBinlogData(
            Table table, 
            MysqlBinlogHandler binlogType, 
            Row rowBefore, 
            Row rowAfter){
        
        this.table = table;
        this.binlogType = binlogType;
        if(rowBefore != null){
            this.rowBefore = Collections.unmodifiableList(rowBefore.getColumns());
        }else{
            this.rowBefore = null;
        }
        if(rowAfter != null){
            this.rowAfter = Collections.unmodifiableList(rowAfter.getColumns());
        }else{
            this.rowAfter = null;
        }
        
    }
    
    public Table getTable(){
        return this.table;
    }
    
    public MysqlBinlogHandler getBinlogEventType(){
        return binlogType;
    }
    
    public List<? extends Column> getRowBefore(){
        return rowBefore;
    }
    
    public List<? extends Column> getRowAfter(){
        return rowAfter;
    }
    
}
