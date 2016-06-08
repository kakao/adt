package com.kakao.adt.mysql.crawler;

import java.util.*;
import java.util.concurrent.*;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kakao.adt.mysql.metadata.*;

public class MysqlTableCrawlTask implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlTableCrawlTask.class);
    
    private final MysqlCrawlProcessor processor;
    private final Table table;
    private final ThreadPoolExecutor threadPool;
    
    private final Index primaryKey;
    private final List<Column> pkColumnList;
    private final List<Object> lastPKValue = new ArrayList<>();
    private final String firstQuery;
    private final String nextQuery;
    
    private final CountDownLatch completeLatch = new CountDownLatch(1);
    private volatile Exception exception;
    
    public MysqlTableCrawlTask(MysqlCrawlProcessor processor, Table table) throws SQLException {
        
        this.processor = processor;
        this.table = table;
        
        this.threadPool = new ThreadPoolExecutor(
                processor.getConfig().workerThreadCountPerTable, 
                processor.getConfig().workerThreadCountPerTable, 
                10, 
                TimeUnit.MINUTES, 
                new LinkedBlockingDeque<>());    
        
        primaryKey = getPrimaryKeyMetaData(this.table);
        pkColumnList = primaryKey.getColumns();
        firstQuery = generateSql(table, processor.getConfig().selectLimitCount, processor.getConfig().selectLockMode, true);
        nextQuery = generateSql(table, processor.getConfig().selectLimitCount, processor.getConfig().selectLockMode, false);
        
    }
    
    private Connection getConnection() throws SQLException{
        Connection conn = processor.getDataSource().getConnection();
        conn.setAutoCommit(false);
        return conn;
    }
    
    private void closeConnection(Connection conn) {
        if(conn != null){
            try{
                if(processor.getConfig().selectLockMode != MysqlSelectLockMode.None){
                    conn.rollback();
                }
            }catch(SQLException e){
                LOGGER.error(e.getMessage(), e);
            }finally{
                try{
                    conn.close();
                }catch(Exception e){
                    LOGGER.error(e.getMessage(), e);
                }
            }
            
        }
    }
    
    /** 
     * NOT RE-USABLE!
     * @throws Exception
     */
    public void crawl() throws Exception{
        
        Connection conn = null;
        try{
            conn = getConnection();
            
            // get first values
            final Statement stmt = conn.createStatement();
            processResult(stmt.executeQuery(firstQuery));
            stmt.close();
            
        }
        catch(Exception e){
            throw e;
        }
        finally{
            closeConnection(conn);
        }
        
        completeLatch.await();
        // wait for finish
        while(this.threadPool.getTaskCount() != 0 && this.threadPool.getActiveCount() != 0){
            Thread.sleep(1);
        }
        if(exception != null){
            throw exception;
        }
        
    }
    
    @Override
    public void run() {
        Connection conn = null;
        try{
            conn = getConnection();
            
            PreparedStatement pstmt = conn.prepareStatement(nextQuery);
            int paramPosition = 0;
            for(int i=0; i<lastPKValue.size(); i++){
                for(int j=0; j<=i; j++){
                    paramPosition++;
                    pstmt.setObject(paramPosition, lastPKValue.get(j));
                }
            }
            processResult(pstmt.executeQuery());
            
            
        }catch(Exception e){
            LOGGER.error(e.getMessage(), e);
            
            exception = e;
            completeLatch.countDown();
        }finally{
            closeConnection(conn);
        }
        
    }
    
    private void processResult(ResultSet rs) throws SQLException{
        // get result as java obj
        List<List<Object>> resultList = getResultList(rs);
        rs.close();
        
        // preload next results
        if(resultList.size() == processor.getConfig().selectLimitCount){
            
            lastPKValue.clear();
            List<Object> lastResult = resultList.get(resultList.size() - 1);
            for(int i=0; i<pkColumnList.size(); i++){
                final Column col = pkColumnList.get(i);
                lastPKValue.add(lastResult.get(col.getPosition()));
            }
            
            this.threadPool.execute(this);
        }
        
        // processing current result
        processor.processData(new MysqlCrawlData(table, resultList));
        
        // if there's no more data, then...
        if(resultList.size() != processor.getConfig().selectLimitCount){
            completeLatch.countDown();
        }
        
    }
    
    //======================================================================
    //
    //      Static Methods
    //
    //======================================================================
    
    public static List<List<Object>> getResultList(ResultSet rs) throws SQLException{
        final ResultSetMetaData metaData = rs.getMetaData();
        final int colCount = metaData.getColumnCount();
        final List<List<Object>> result = new ArrayList<>();
        
        while(rs.next()){
            final List<Object> row = new ArrayList<>();
            for(int i=1; i<=colCount; i++){
                Object obj = rs.getObject(i);
                row.add(obj);
            }
            result.add(row);
        }
        return result;
    }
    
    public static String generateSql(Table table, int limitCount, MysqlSelectLockMode lockMode, boolean isFirstCrawl){
        
        final Index primary = getPrimaryKeyMetaData(table);
        final List<Column> colList = primary.getColumns();
        
        String selectSql = String.format("SELECT * FROM `%s`.`%s` \n", table.getDatabase().getName(), table.getName());

        if(! isFirstCrawl){
            
            String whereSql = " WHERE ";
            for(int i=0; i<colList.size(); i++){
                whereSql += generateSqlWhereClause(colList, i + 1)
                        + (i < colList.size() - 1 ? "\n OR " : "\n");
                
            }
            
            selectSql += whereSql;
            
        }
        
        String orderLimitSql = " ORDER BY ";
        for(int i=0; i<colList.size(); i++){
            final Column col = colList.get(i);
            
            orderLimitSql += String.format("`%s` ASC", col.getName()) + 
                    (i < colList.size() - 1 ? 
                        ", " : 
                        "\n LIMIT " + limitCount + "\n" + lockMode.getSql());
        }
        selectSql += orderLimitSql;
        
        return selectSql;
    }
    
    /**
     * @param colList
     * @param order
     * @param indexDepth start from 1
     * @return
     */
    private static String generateSqlWhereClause(List<Column> colList, int indexDepth){
        String sql = "( ";
        for(int i=0; i<indexDepth; i++){
            final Column col = colList.get(i);
            sql += String.format("`%s`", col.getName());
            if(i < indexDepth - 1){
                sql += " = ? AND ";
            }
            else{
                sql += " > ? )";
            }
        }
        
        return sql;
        
    }
    
    private static Index getPrimaryKeyMetaData(Table table){
        for(Index idx : table.getIndexes()){
            if(idx.getName().equalsIgnoreCase("primary")){
                return idx;
            }
        }
        throw new IllegalStateException("PK doesn't exist. (table=" + table.getName() + ")");
    }

}
