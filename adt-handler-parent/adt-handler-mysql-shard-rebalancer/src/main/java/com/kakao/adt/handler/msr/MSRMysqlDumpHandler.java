package com.kakao.adt.handler.msr;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.script.ScriptException;

import com.kakao.adt.handler.MysqlCrawlProcessorHandler;
import com.kakao.adt.misc.Tuple2;
import com.kakao.adt.mysql.crawler.MysqlCrawlData;
import com.kakao.adt.mysql.metadata.Table;

public class MSRMysqlDumpHandler extends AbstractMysqlHandler implements MysqlCrawlProcessorHandler {

    public MSRMysqlDumpHandler() throws Exception {
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

    public final ConcurrentHashMap<Tuple2<Table, Integer>, String> cachedQuery = new ConcurrentHashMap<>();
    
    @Override
    public void processData(MysqlCrawlData data) throws Exception {
        
        final List<List<Object>> rowList = data.getAllRows();
        final Table table = data.getTable(); 
        
        final List<List<List<Object>>> rowListByShard = new ArrayList<>();
        for(int i=0; i<shardCount; i++){
            rowListByShard.add(new ArrayList<>());
        }
        
        for(int i=0; i<rowList.size(); i++){
            List<Object> row = rowList.get(i);
            int shardId = getShardIndex(table, row);
            rowListByShard.get(shardId).add(row);
        }
        
        for(int i=0; i<shardCount; i++){
            insertBulk(i, table, rowListByShard.get(i));
        }
        
    }

    public void insertBulk(int shardIndex, Table table, List<List<Object>> rowList) throws Exception{
        
        if (rowList.size() == 0){
            return;
        }
        
        Tuple2<Table, Integer> cacheKey = new Tuple2<>(table, rowList.size()); 
        String sql = cachedQuery.get(cacheKey);
        if(sql == null){
            sql = "REPLACE INTO " + table.getName() + " VALUES (";
            
            for(int i=0; i<rowList.size(); i++){
                for(int j=0; j<table.getColumns().size(); j++){
                    sql += (j == table.getColumns().size() - 1 ? "?" : "?, ");
                }
                sql += (i == rowList.size() - 1 ? ")" : "), (");
            }
            
            cachedQuery.put(cacheKey, sql);
        }
        
        List<Object> pstmtParams = new ArrayList<>();
        for(int i=0; i<rowList.size(); i++){
            pstmtParams.addAll(rowList.get(i));
        }
//        System.out.println(sql);
//        System.out.println(pstmtParams.toString());
        executeUpdate(sql, shardIndex, pstmtParams);
        
    }
    
    
}
