package com.kakao.adt.handler.msr;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kakao.adt.mysql.metadata.Index;
import com.kakao.adt.mysql.metadata.Table;

public class AbstractMysqlHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger("MySQL-Shard-Rebalancer");
    
    public static final String SCRIPT_ENGINE_NAME = "nashorn";
    public static final String PROP_SHARD_CNT = "com.kakao.adt.handler.msr.shardCount";
    public static final String PROP_SHARD_URL = "com.kakao.adt.handler.msr.shard.%d.url";
    public static final String PROP_SHARD_USERNAME = "com.kakao.adt.handler.msr.shard.%d.username";
    public static final String PROP_SHARD_PASSWORD = "com.kakao.adt.handler.msr.shard.%d.password";
    public static final String PROP_SCRIPT = "com.kakao.adt.handler.msr.customScript";
    public static final String PROP_TEST_MODE = "com.kakao.adt.handler.msr.testMode";
    
    public final int shardCount;
//    public final ScriptEngine scriptEngine;
    public final List<BasicDataSource> dataSourceList = new ArrayList<>();
    
    public final ConcurrentHashMap<Table, String> replaceCachedQuery = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Table, String> insertIgnoreCachedQuery = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Table, String> insertCachedQuery = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Table, String> deleteByPkCachedQuery = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Index, String> deleteByIdxCachedQuery = new ConcurrentHashMap<>();

    
    public AbstractMysqlHandler() throws Exception{
        shardCount = Integer.parseInt(System.getProperty(PROP_SHARD_CNT));
        for(int i=0; i<shardCount; i++){
            final BasicDataSource ds = new BasicDataSource();
            ds.setMaxTotal(1024);
            ds.setMaxIdle(1024);
            ds.setMinIdle(0);
            ds.setUrl(System.getProperty(String.format(PROP_SHARD_URL, i)));
            ds.setUsername(System.getProperty(String.format(PROP_SHARD_USERNAME, i)));
            ds.setPassword(System.getProperty(String.format(PROP_SHARD_PASSWORD, i)));
            dataSourceList.add(ds);
        }
        
//        final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
//        scriptEngine = scriptEngineManager.getEngineByName(SCRIPT_ENGINE_NAME);
//        scriptEngine.eval(System.getProperty(PROP_SCRIPT));
        
    }
    
    public void replace(Table table, List<Object> row) throws Exception{
        final int shardIndex = getShardIndex(table, row);
        String sql = replaceCachedQuery.get(table);
        if(sql == null){
            sql = "REPLACE INTO " + table.getName() + " VALUES(";
            for(int i=0; i<row.size(); i++){
                sql += (i == row.size() - 1 ? "?)" : "?, ");
            }
            replaceCachedQuery.putIfAbsent(table, sql);
        }
        
        executeUpdate(sql, shardIndex, row);
    }
    
    public void insertIgnore(Table table, List<Object> row) throws Exception{
        final int shardIndex = getShardIndex(table, row);
        String sql = insertIgnoreCachedQuery.get(table);
        if(sql == null){
            sql = "INSERT IGNORE INTO " + table.getName() + " VALUES(";
            for(int i=0; i<row.size(); i++){
                sql += (i == row.size() - 1 ? "?)" : "?, ");
            }
            insertIgnoreCachedQuery.putIfAbsent(table, sql);
        }
     
        executeUpdate(sql, shardIndex, row);
        
    }
    
    public void insert(Table table, List<Object> row) throws Exception{
        final int shardIndex = getShardIndex(table, row);
        String sql = insertCachedQuery.get(table);
        if(sql == null){
            sql = "INSERT INTO " + table.getName() + " VALUES(";
            for(int i=0; i<row.size(); i++){
                sql += (i == row.size() - 1 ? "?)" : "?, ");
            }
            insertCachedQuery.putIfAbsent(table, sql);
        }
     
        executeUpdate(sql, shardIndex, row);
     
    }
    
    public void deleteByPk(Table table, List<Object> row) throws Exception{
        final int shardIndex = getShardIndex(table, row);

        String sql = deleteByPkCachedQuery.get(table);
        if(sql == null){
            sql = "DELETE FROM " + table.getName() + " WHERE ";
            final Index pkIndex = findPrimaryKeyIndex(table);
            for(int i=0; i<pkIndex.getColumns().size(); i++){
                sql += pkIndex.getColumn(i).getName()
                    + (i == pkIndex.getColumns().size() - 1 ? " = ?;" : " = ? AND ");
            }
            deleteByPkCachedQuery.putIfAbsent(table, sql);
        }
        
        executeUpdate(sql, shardIndex, getPrimaryKeyData(table, row));
    }
    
    public void deleteByIdx(Index index, List<Object> row) throws Exception {
        final Table table = index.getTable();
        final int shardIndex = getShardIndex(table, row);

        String sql = deleteByIdxCachedQuery.get(index);
        if (sql == null) {
            sql = "DELETE FROM " + table.getName() + " WHERE ";

            for (int i = 0; i < index.getColumns().size(); i++) {
                sql += index.getColumn(i).getName()
                        + (i == index.getColumns().size() - 1 ? " = ?" : " = ? AND ");
            }
            deleteByIdxCachedQuery.putIfAbsent(index, sql);
        }

        executeUpdate(sql, shardIndex, getPrimaryKeyData(table, row));

    }
    
    public int executeUpdate(String sql, int shardIndex, List<Object> parameters) throws Exception{
        
        final int maxTryCount = 3;
        Exception lastException = null;
        for(int currentTryCount = 1; currentTryCount <= maxTryCount; currentTryCount ++){
            
            Connection conn = null;
            try{
                conn = dataSourceList.get(shardIndex).getConnection();
                conn.setAutoCommit(true);
                
                final PreparedStatement pstmt = conn.prepareStatement(sql);
                for(int i=0; i<parameters.size(); i++){
                    pstmt.setObject(i+1, parameters.get(i));
                }
                int result = pstmt.executeUpdate();
                pstmt.clearParameters();
                return result;
            }catch(Exception e){
                
                LOGGER.error("Failed to executeUpdate. "
                        + "(" + currentTryCount + "/" + maxTryCount + ") " 
                        + ", sql=" + sql 
                        + ", shardIndex=" + shardIndex 
                        + ", param=" + parameters, e);
                lastException = e;
                
            }finally{
                if(conn != null){
                    conn.close();
                }
            }
        }
        
        if(lastException != null){
            throw lastException;
        }
        else{
            throw new IllegalStateException("retry failed, but no exception. you should find this bug and fix it!!");
        }
        
    }
    
    public int getShardIndex(Table table, List<Object> row) throws Exception{
        
        final List<Object> pkDataList = getPrimaryKeyData(table, row);
        
        //final Object jsTableObj = scriptEngine.eval(table.getDatabase().getName() + "." + table.getName());
        //final Object result = ((Invocable)scriptEngine).invokeMethod(jsTableObj, "getShardIndex", pkDataList, shardCount);
        
        //return ((Number)result).intValue();
        
        return ((Number)pkDataList.get(0)).intValue() % shardCount;
    }
    

    public static List<Object> getIndexKeyData(Index index, List<Object> allDataList){
        
        List<Object> result = new ArrayList<>();
        for(int j=0; j<index.getColumns().size(); j++){
            Object colData = allDataList.get(index.getColumn(j).getPosition());
            result.add(colData);
        }
        return result;
        
    }
    
    public static List<Object> getPrimaryKeyData(Table table, List<Object> allDataList){
        
        final Index index = findPrimaryKeyIndex(table);
        return getIndexKeyData(index, allDataList);
        
    }
    
    public static Index findPrimaryKeyIndex(Table table){
        for(int i=0; i<table.getIndexes().size(); i++){
            Index index = table.getIndex(i);
            if(index.getName().equals("PRIMARY")){
                return index;
            }
        }
        throw new IllegalStateException("NO PK in " + table.getDatabase().getName() + "." + table.getName());
    }
    
}
