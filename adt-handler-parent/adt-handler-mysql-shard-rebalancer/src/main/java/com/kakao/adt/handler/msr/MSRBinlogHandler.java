package com.kakao.adt.handler.msr;

import java.util.ArrayList;
import java.util.List;
import com.google.code.or.common.glossary.Column;
import com.kakao.adt.handler.MysqlBinlogProcessorHandler;
import com.kakao.adt.mysql.binlog.MysqlBinlogData;
import com.kakao.adt.mysql.binlog.MysqlBinlogHandler;
import com.kakao.adt.mysql.metadata.Index;
import com.kakao.adt.mysql.metadata.Table;

public class MSRBinlogHandler extends AbstractMysqlHandler implements MysqlBinlogProcessorHandler {

    public MSRBinlogHandler() throws Exception{
        super();
    }
    
    @Override
    public void beforeStart() {
        // TODO Auto-generated method stub

    }

    @Override
    public void afterStop() {
        // TODO Auto-generated method stub

    }

    @Override
    public void onException(Exception e) {
        // TODO Auto-generated method stub

    }

    @Override
    public void processData(MysqlBinlogData data) throws Exception{
        
        final MysqlBinlogHandler type = data.getBinlogEventType();
        
        final Table table = data.getTable();
        
        /*
        final int maxRetryCount = 3;
        Exception lastException = null;
        for(int i=0; i<maxRetryCount; i++){
            try{
                switch(type){
                    case WRITE_ROWS_EVENT_HANDLER:
                    case WRITE_ROWS_EVENT_V2_HANDLER:
                        replace(table, getValuesFromColumnList(data.getRowAfter()));
                        break;
                        
                    case DELETE_ROWS_EVENT_HANDLER:
                    case DELETE_ROWS_EVENT_V2_HANDLER:
                        delete(table, getValuesFromColumnList(data.getRowBefore()));
                        break;
                        
                    case UPDATE_ROWS_EVENT_HANDLER:
                    case UPDATE_ROWS_EVENT_V2_HANDLER:
                        delete(table, getValuesFromColumnList(data.getRowBefore()));
                        replace(table, getValuesFromColumnList(data.getRowAfter()));
                        break;
                        
                    default:
                }
                
                return;
            }catch(Exception e){
                LOGGER.error("CurrentTryCount=" + (i+1) + "/" + maxRetryCount , e);
                try{
                    Thread.sleep(1000);
                }catch(Exception e2){
                    
                }
                lastException = e;
            }
        }
        
        if(lastException != null){
            throw lastException;
        }*/
        
        switch(type){
            case WRITE_ROWS_EVENT_HANDLER:
            case WRITE_ROWS_EVENT_V2_HANDLER:
                replace(table, getValuesFromColumnList(data.getRowAfter()));
                break;
                
            case UPDATE_ROWS_EVENT_HANDLER:
            case UPDATE_ROWS_EVENT_V2_HANDLER:
                final List<Object> rowBefore = getValuesFromColumnList(data.getRowBefore());
                final List<Object> rowAfter = getValuesFromColumnList(data.getRowAfter());
                if(!getPrimaryKeyData(table, rowBefore).equals(getPrimaryKeyData(table, rowAfter))){
                    deleteByPk(table, rowBefore);
                }
                replace(table, rowAfter);
                break;
                
            case DELETE_ROWS_EVENT_HANDLER:
            case DELETE_ROWS_EVENT_V2_HANDLER:
                deleteByPk(table, getValuesFromColumnList(data.getRowBefore()));
                break;
        }


    }
    
    public List<Object> getValuesFromColumnList(List<? extends Column> colList){
        List<Object> result = new ArrayList<>();
        for(Column col : colList){
            result.add(col.getValue());
        }
        return result;
    }
    
}
