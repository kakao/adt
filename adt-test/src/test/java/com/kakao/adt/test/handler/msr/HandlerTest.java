package com.kakao.adt.test.handler.msr;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kakao.adt.WorkerMain;
import com.kakao.adt.WorkerMain.WorkerType;
import com.kakao.adt.handler.msr.MSRBinlogHandler;
import com.kakao.adt.handler.msr.MSRTableCrawlHandler;
import com.kakao.adt.mysql.binlog.MysqlBinlogProcessorConfig;
import com.kakao.adt.mysql.crawler.MysqlCrawlProcessorConfig;
import com.kakao.adt.mysql.crawler.MysqlSelectLockMode;
import com.kakao.adt.test.TestUtil;
import com.kakao.adt.test.tool.DMLQueryTool;
import com.kakao.adt.test.tool.DataIntegrityTestTool;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.io.File;
import java.sql.*;

import static com.kakao.adt.test.TestUtil.*;
import static com.kakao.adt.test.tool.DMLQueryTool.*;

public class HandlerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandlerTest.class);
    private static MysqlDataSource dataSource;
    
    @BeforeClass
    public static void beforeClass() throws Exception{
        initializeTestEnv();
        
        final int sleepCount = 30;
        
        LOGGER.debug("sleep " + sleepCount + "sec for generating enough test data");
        for(int i=1; i<=sleepCount; i++){
            LOGGER.debug(i + "/" + sleepCount);
            Thread.sleep(1000);
        }
        
        dataSource = new MysqlDataSource();
        dataSource.setURL(DB_URL);
        dataSource.setUser(DB_USER);
        dataSource.setPassword(DB_PW);
        
        for(int i=0; i<SHARD_COUNT; i++){
            
            Connection connDest = DriverManager.getConnection(DB_DEST_URL_LIST_WITHOUT_SCHEMA[i], DB_USER, DB_PW);
            try{
                Statement stmt = connDest.createStatement();
                stmt.execute("DROP DATABASE IF EXISTS " + DB_DEST_SCHEMA_LIST[i]);
                stmt.execute("CREATE DATABASE " + DB_DEST_SCHEMA_LIST[i]);
                stmt.execute(String.format(TestUtil.getResourceAsString("sql/create_table_adt_test_1.sql"), DB_DEST_SCHEMA_LIST[i]));
                stmt.execute(String.format(TestUtil.getResourceAsString("sql/create_table_adt_test_2.sql"), DB_DEST_SCHEMA_LIST[i]));
                stmt.execute(String.format(TestUtil.getResourceAsString("sql/create_table_adt_test_3.sql"), DB_DEST_SCHEMA_LIST[i]));
            }finally{
                connDest.close();
            }
            
        }
            
    }
    
    @Test
    public void test() throws Exception{
        test_tableCrawlHandler();
        
        Thread binlogProcStarter = new Thread(new Runnable() {
            public void run() {
                try {
                    test_binlogHandler();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    Assert.assertTrue(false);
                }
            }
        });
        
        binlogProcStarter.start();
        
        final int dataIntegrityCheckCount = 3;
        final int sleepTime = 30;
        
        Properties prop = new Properties();
        prop.setProperty(PROP_MSR_SRC_URL, DB_URL + "/" + DB_TEST_SCHEMA);
        prop.setProperty(PROP_MSR_SRC_USERNAME, DB_USER);
        prop.setProperty(PROP_MSR_SRC_PASSWROD, DB_PW);
        prop.setProperty(PROP_MSR_DEST_COUNT, SHARD_COUNT+"");
        for(int i=0; i<SHARD_COUNT; i++){
            prop.setProperty(String.format(DMLQueryTool.PROP_MSR_DEST_URL, i), DB_DEST_URL_LIST[i]);
            prop.setProperty(String.format(DMLQueryTool.PROP_MSR_DEST_USERNAME, i), DB_USER);
            prop.setProperty(String.format(DMLQueryTool.PROP_MSR_DEST_PASSWORD, i), DB_PW);
        }
        final DataIntegrityTestTool integrityTest = new DataIntegrityTestTool(prop);
        
        
        for(int i=0; i<dataIntegrityCheckCount; i++){
            
            for(int j=0; j<sleepTime; j++){
                LOGGER.debug("sleep: {}/{}", j, sleepTime);
                Thread.sleep(1000);
            }
            
            Assert.assertTrue(integrityTest.runTest());
            
        }
        
        WorkerMain.getProcessorInstance().stop();
        binlogProcStarter.join();
        
    }
    
    
    private void test_tableCrawlHandler() throws Exception{
        final MysqlCrawlProcessorConfig config = new MysqlCrawlProcessorConfig();
        config.host = DB_HOST;
        config.port = DB_PORT;
        config.user = DB_USER;
        config.password = DB_PW;
        config.handlerClassName = MSRTableCrawlHandler.class.getName();
        config.jdbcConnParam = "";
        config.dbcp2Properties = new Properties();
        config.tableList = Arrays.asList(DB_TEST_SCHEMA + ".adt_test_1", DB_TEST_SCHEMA + ".adt_test_2");
        config.selectLimitCount = 500;
        config.selectLockMode = MysqlSelectLockMode.None;
        config.concurrentCrawlCount = 2;
        config.workerThreadCountPerTable = 512;
        
        config.dbcp2Properties.setProperty("maxTotal", "1024");
        config.dbcp2Properties.setProperty("maxIdle", "1024");
        
        executeWorker(config, WorkerType.MysqlTableCrawler);
        
    }
    
    private void test_binlogHandler() throws Exception{
        
        // gen config
        final MysqlBinlogProcessorConfig config = new MysqlBinlogProcessorConfig();
        config.host = DB_HOST;
        config.port = DB_PORT;
        config.user = DB_USER;
        config.password = DB_PW;
        config.handlerClassName = MSRBinlogHandler.class.getName();
        config.databaseList = Arrays.asList(DB_TEST_SCHEMA);
        config.mysqlSlaveServerId = DB_SLAVE_SERVER_ID;
        config.binlogFileName = ""; // start from first log file
        config.binlogFilePosition = 4;
        config.taskQueueCount = 1024;
        config.eventBufferSize = 1000*1000;
        config.workerThreadCorePoolSize = 512;
        
        executeWorker(config, WorkerType.MysqlBinlogReceiver);
        
    }
    
    private void executeWorker(Object config, WorkerType type) throws Exception{
        final String configFileName = "test_config.json";
        
        // create config file
        final File configFile = new File(System.getProperty("user.dir") + "/" + configFileName);
        configFile.delete();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(configFile, config);
        
        // handler config
        System.setProperty(MSRBinlogHandler.PROP_SHARD_CNT, SHARD_COUNT + "");
        for(int i=0; i<SHARD_COUNT; i++){
            System.setProperty(String.format(MSRBinlogHandler.PROP_SHARD_URL,i), DB_DEST_URL_LIST[i]);
            System.setProperty(String.format(MSRBinlogHandler.PROP_SHARD_USERNAME,i), DB_USER);
            System.setProperty(String.format(MSRBinlogHandler.PROP_SHARD_PASSWORD,i), DB_PW);
        }
        LOGGER.info(System.getProperty(MSRBinlogHandler.PROP_SCRIPT));
        
        // worker config
        System.setProperty(WorkerMain.PROP_KEY_WORKER_TYPE, type.toString());
        System.setProperty(WorkerMain.PROP_KEY_WORKER_CONFIG_FILE_PATH, configFileName);
                
        // start
        LOGGER.info("\n" + System.getProperties());
        WorkerMain.main(new String[]{});
        
        configFile.delete();
    }
    
    
    public static Connection getConnection(String url, String username, String password) throws SQLException{
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(url);
        ds.setUser(username);
        ds.setPassword(password);
        return ds.getConnection();
    }
    
    public static void truncateTable(String url, String username, String password, String tableName) throws SQLException{
        Connection conn = getConnection(url, username, password);
        try{
            Statement stmt = conn.createStatement();
            stmt.execute("TRUNCATE TABLE " + tableName);
        }finally{
            conn.close();
        }
    }
    
    public static List<String> getBinlogFileNameList(String url, String username, String password) throws SQLException{
        Connection conn = getConnection(url, username, password);
        try{
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW BINARY LOGS");
            
            List<String> result = new ArrayList<>();
            while(rs.next()){
                result.add(rs.getString(1));
            }
            
            return result;
        }finally{
            conn.close();
        }
    }
    
    
}
