package com.kakao.adt.test.worker.mysql.binlog;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.glossary.Metadata;
import com.kakao.adt.mysql.binlog.MysqlBinlogProcessor;
import com.kakao.adt.mysql.binlog.MysqlBinlogData;
import com.kakao.adt.mysql.binlog.MysqlBinlogHandler;
import com.kakao.adt.mysql.binlog.MysqlBinlogProcessorConfig;
import com.kakao.adt.mysql.binlog.MysqlBinlogTask;
import com.kakao.adt.mysql.metadata.Column;
import com.kakao.adt.mysql.metadata.Index;
import com.kakao.adt.mysql.metadata.MysqlServerInfo;
import com.kakao.adt.mysql.metadata.Table;
import com.kakao.adt.test.TestUtil;
import com.kakao.adt.test.worker.TestMysqlBinlogProcessorHandler;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import static com.kakao.adt.test.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MysqlBinlogProcessorDBCopyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogProcessorDBCopyTest.class);
    private static final String TEMP_SCHEMA = DB_TEST_SCHEMA + "_temp";
    private static MysqlDataSource dataSource;
    
    private final AtomicLong accTaskProcCount = new AtomicLong(0);
    private long startTime = System.currentTimeMillis();
    private long exInputCnt = 0;
    private long exExecuteCnt = 0;
    private long exOutputCnt = 0;
    private long exTaskProcCnt = 0;
    
    private MysqlBinlogProcessor processor;
    
    @BeforeClass
    public static void beforeClass() throws Exception{
        initializeTestEnv();
        
        final int sleepCount = 30;
        
        LOGGER.debug("sleep " + sleepCount + "sec for generating enough test data");
        for(int i=1; i<=sleepCount; i++){
            LOGGER.debug(i + "/" + sleepCount);
            Thread.sleep(1000);
        }
        
        // create dataSource
        dataSource = new MysqlDataSource();
        dataSource.setUrl(DB_URL);
        dataSource.setUser(DB_USER);
        dataSource.setPassword(DB_PW);
        
        
    }
    
    @Test
    public void test_copyWithBinlogProcessor() throws Exception{

        final String startLogFileName = new MysqlServerInfo(DB_HOST, DB_PORT, DB_USER, DB_PW, Collections.emptyList(), true).getLastBinaryLogFile().getLogName();
        Connection conn = dataSource.getConnection();
        
        Statement stmt = conn.createStatement();
        stmt.execute("DROP DATABASE IF EXISTS " + TEMP_SCHEMA);
        stmt.execute("CREATE DATABASE " + TEMP_SCHEMA);
        stmt.execute(String.format("CREATE TABLE %s.adt_test_1 LIKE %s.adt_test_1", TEMP_SCHEMA, DB_TEST_SCHEMA));
        stmt.execute(String.format("INSERT INTO %s.adt_test_1 SELECT * FROM %s.adt_test_1", TEMP_SCHEMA, DB_TEST_SCHEMA));
        stmt.close();
      
        
        final MysqlBinlogProcessorConfig config = new MysqlBinlogProcessorConfig();
        config.host = DB_HOST;
        config.port = DB_PORT;
        config.user = DB_USER;
        config.password = DB_PW;
        config.databaseList = Arrays.asList(DB_TEST_SCHEMA);
        config.mysqlSlaveServerId = DB_SLAVE_SERVER_ID;
        config.binlogFileName = startLogFileName;
        config.binlogFilePosition = 4;
        config.taskQueueCount = 128;
        config.eventBufferSize = 100*1000;
        config.workerThreadCorePoolSize = 512;
        config.handlerClassName = TestMysqlBinlogProcessorHandler.class.getName();
        
        processor = new TestBinlogProcessor(config);
        processor.start();
        
        for(int i=0; i<30; i++){
            Thread.sleep(1000);
            printStat();
            
        }
        
        // check data integrity
        conn.setAutoCommit(false);
        stmt = conn.createStatement();
        
        LOGGER.info("Lock src table & select all of src table");
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s.adt_test_1 FOR UPDATE", DB_TEST_SCHEMA));
        List<List<String>> result1 = getResultSet(rs);
        LOGGER.info("src size: " + result1.size());

        Connection conn2 = dataSource.getConnection();
        Statement stmt2 = conn2.createStatement();
        
        int exHash = 0;
        List<List<String>> result2;
        LOGGER.info("Waiting for finishing binlog processor...");
        while(true){
            
            LOGGER.info("select all of dest table");
            result2 = getResultSet(stmt2.executeQuery(String.format("SELECT * FROM %s.adt_test_1", TEMP_SCHEMA)));
            LOGGER.info("ex_hash:{}, cur_hash:{}", exHash, result2.hashCode());
            
            if(exHash == result2.hashCode()){
                break;
            }
            
            for(int i=0; i<10; i++){
                printStat();
                Thread.sleep(1000);
            }
            
            exHash = result2.hashCode();
            continue;
        }
        
        assertEquals(result1.size(), result2.size());
        for(int i=0; i<result1.size(); i++){
            List<String> row1 = result1.get(i);
            List<String> row2 = result2.get(i);
            
            for(int j=0; j<row1.size()-2; j++){
                assertEquals(row1.get(j), row2.get(j));
            }
            
        }
        
        conn.rollback();
        
        processor.stop();
        
    }
    
    private void printStat(){
        long deltaTime = System.currentTimeMillis() - startTime;
        long curInputCnt = processor.getInputCount();
        long curExecuteCnt = processor.getExecuteCount();
        long curOutputCnt = processor.getOutputCount();
        long curTaskProcCnt = accTaskProcCount.get();
        
        System.out.println(String.format("%d, %d, %d, %d", curInputCnt, curExecuteCnt, curOutputCnt, curTaskProcCnt));
        System.out.println(String.format(
                "AVG   %d i/msec,   %d e/msec,   %d o/msec,   %d p/msec", 
                curInputCnt/deltaTime,
                curExecuteCnt/deltaTime,
                curOutputCnt/deltaTime,
                curTaskProcCnt/deltaTime));
        
        assertTrue(exInputCnt <= curInputCnt);
        assertTrue(exExecuteCnt <= curExecuteCnt);
        assertTrue(exOutputCnt <= curOutputCnt);
        assertTrue(exTaskProcCnt <= curTaskProcCnt);
        
        exInputCnt = curInputCnt;
        exExecuteCnt = curExecuteCnt;
        exOutputCnt = curOutputCnt;
        exTaskProcCnt = curTaskProcCnt;
    }
    
    private class TestBinlogProcessor extends MysqlBinlogProcessor{

        final ThreadLocal<Connection> connThreadLocal = new ThreadLocal<>();
        final ThreadLocal<PreparedStatement> replaceThreadLocal = new ThreadLocal<>();
        final ThreadLocal<PreparedStatement> deleteThreadLocal = new ThreadLocal<>();
        
        public TestBinlogProcessor(MysqlBinlogProcessorConfig _config)
                throws Exception {
            super(_config);
        }

        @Override
        public void processData(MysqlBinlogData data) throws Exception {
            accTaskProcCount.incrementAndGet();
            final String dbName = data.getTable().getDatabase().getName();
            final String tableName = data.getTable().getName();
            
            if( ! dbName.equalsIgnoreCase(DB_TEST_SCHEMA)){
                return;
            }
            if( ! tableName.equalsIgnoreCase("adt_test_1")){
                return;
            }
            final int maxRetryCount = 3;
            for(int curTryCount=1; curTryCount<=maxRetryCount; curTryCount++){
                try {
                    
                    Connection conn = connThreadLocal.get();
                    if(conn == null || conn.isClosed() == true){
                        conn = dataSource.getConnection();
                        replaceThreadLocal.remove();
                        deleteThreadLocal.remove();
                        
                        connThreadLocal.set(dataSource.getConnection());
                        replaceThreadLocal.set(conn.prepareStatement(getReplaceQuery(data.getTable())));
                        deleteThreadLocal.set(conn.prepareStatement(getDeleteQuery(data.getTable())));
                        
                    }
                    final Table table = data.getTable();
                    PreparedStatement pstmt;
                    Index pk = getPrimaryKeyIndex(table);
                    
                    switch(data.getBinlogEventType()){
                        case WRITE_ROWS_EVENT_HANDLER:
                        case WRITE_ROWS_EVENT_V2_HANDLER:
                            
                            pstmt = replaceThreadLocal.get();
                            for(int i=0; i<table.getColumns().size(); i++){
                                pstmt.setObject(i+1, data.getRowAfter().get(i).getValue());
                            }
                            pstmt.execute();
                            pstmt.clearParameters();
                            
                            break;
    
                        case UPDATE_ROWS_EVENT_HANDLER:
                        case UPDATE_ROWS_EVENT_V2_HANDLER:
                            
                            pstmt = deleteThreadLocal.get();
                            for(int i=0; i<pk.getColumns().size(); i++){
                                pstmt.setObject(i+1, data.getRowBefore().get(pk.getColumn(i).getPosition()).getValue());
                            }
                            pstmt.execute();
                            pstmt.clearParameters();
                            
                            pstmt = replaceThreadLocal.get();
                            for(int i=0; i<table.getColumns().size(); i++){
                                pstmt.setObject(i+1, data.getRowAfter().get(i).getValue());
                            }
                            pstmt.execute();
                            pstmt.clearParameters();
                            
                            break;
                            
                        case DELETE_ROWS_EVENT_HANDLER:
                        case DELETE_ROWS_EVENT_V2_HANDLER:
                            
                            
                            pstmt = deleteThreadLocal.get();
                            for(int i=0; i<pk.getColumns().size(); i++){
                                pstmt.setObject(i+1, data.getRowBefore().get(pk.getColumn(i).getPosition()).getValue());
                            }
                            pstmt.execute();
                            pstmt.clearParameters();
                            
                            break;
                            
                        default:
                            LOGGER.error("unknown type. " + data.getBinlogEventType());
                            assertTrue(false);
                            break;
                    }
                
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    LOGGER.error("Current Retry Count: " + curTryCount + "/" + maxRetryCount);
                    
                    if(curTryCount == maxRetryCount){
                        throw e;
                    }
                }
            }
            
            
        }
    }
    
    public static List<List<String>> getResultSet(ResultSet rs) throws SQLException{
        List<List<String>> result = new ArrayList<>();
        
        while(rs.next()){
            List<String> row = new ArrayList<>();
            for(int i=1; i<= rs.getMetaData().getColumnCount(); i++){
                row.add(rs.getString(i));
            }
            result.add(row);
        }
        return result;
        
    }
    
    public static String getReplaceQuery(Table table){
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("REPLACE INTO " + TEMP_SCHEMA + ".")
            .append(table.getName())
            .append(" VALUES(");
        
        List<Column> columnList = table.getColumns();
        for(int i=0; i<columnList.size(); i++){
            sqlBuilder.append(i == columnList.size() - 1 ? "?)" : "?, ");
        }
        return sqlBuilder.toString();
    }
    
    public static String getDeleteQuery(Table table){
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM " + TEMP_SCHEMA + ".")
            .append(table.getName());
        
        List<Column> pkColumnList = getPrimaryKeyIndex(table).getColumns();
        assertTrue(pkColumnList != null);
        
        for(int i=0; i<pkColumnList.size(); i++){
            
            sqlBuilder.append(i == 0 ? " WHERE " : " AND ");
            
            Column col = pkColumnList.get(i);
            sqlBuilder.append(col.getName())
                .append("=?");
        }
        return sqlBuilder.toString();
    }
    
    public static Index getPrimaryKeyIndex(Table table){
        for(Index index : table.getIndexes()){
            if(index.getName().equalsIgnoreCase("primary")){
                return index;
            }
        }
        return null;
    }
    
}
