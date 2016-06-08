package com.kakao.adt.test.worker.mysql.crawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kakao.adt.mysql.crawler.MysqlCrawlProcessor;
import com.kakao.adt.mysql.crawler.MysqlCrawlData;
import com.kakao.adt.mysql.crawler.MysqlCrawlProcessorConfig;
import com.kakao.adt.mysql.crawler.MysqlSelectLockMode;
import com.kakao.adt.mysql.crawler.MysqlTableCrawlTask;
import com.kakao.adt.mysql.metadata.Table;
import com.kakao.adt.test.worker.TestMysqlCrawlProcessorHandler;

import static com.kakao.adt.test.TestUtil.*;
import static org.junit.Assert.*;

public class MysqlCrawlerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlCrawlerTest.class);
    
    private Connection conn = null;
    
    private MysqlCrawlProcessor getTestCrawler() throws Exception{
        
        MysqlCrawlProcessorConfig config = new MysqlCrawlProcessorConfig();
        config.host = DB_HOST;
        config.port = DB_PORT;
        config.user = DB_USER;
        config.password = DB_PW;
        config.jdbcConnParam = "";
        config.dbcp2Properties = new Properties();
        config.tableList = Arrays.asList(DB_TEST_SCHEMA + ".adt_test_1");
        config.selectLimitCount = 100;
        config.selectLockMode = MysqlSelectLockMode.ForUpdate;
        config.concurrentCrawlCount = 1;
        config.workerThreadCountPerTable = 4;
        config.handlerClassName = TestMysqlCrawlProcessorHandler.class.getName();
        
        return new MysqlCrawlProcessor(config) {

            @Override
            public void processData(MysqlCrawlData data) {
                
            }
            
            @Override
            public void onException(Exception e) {
                LOGGER.error(e.getMessage(), e);
                stop();
                
            }
        };
    }
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeTestEnv();
    }
    
    @Before
    public void before() throws Exception {
        conn = testDataSource.getConnection();
    }
    
    @After
    public void after() throws Exception{
        if(conn != null){
            conn.close();
            conn = null;
        }
    }
    
    
    @Test
    public void test_gen_sql() throws Exception{
        
        conn.setAutoCommit(false);
        
        final MysqlCrawlProcessor processor = getTestCrawler();
        final Table table = processor.getServerInfo().getDatabase(DB_TEST_SCHEMA).getTable("adt_test_1");
        
        // first crawling
        String sql = MysqlTableCrawlTask.generateSql(table, 10, MysqlSelectLockMode.ForUpdate, true);
        System.out.println(sql);
        
        ResultSet rs = conn.createStatement().executeQuery(sql);
        List<List<Object>> result1 = MysqlTableCrawlTask.getResultList(rs);
        System.out.println(result1);
        assertTrue(result1.size() == 10);
                
        Object obj1 = result1.get(0).get(0); //no column
        Object obj2 = result1.get(0).get(1); //seq column
        
        // second crawling
        sql = MysqlTableCrawlTask.generateSql(table, 9, MysqlSelectLockMode.ForUpdate, false);
        System.out.println(sql);
        
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setObject(1, obj1);
        pstmt.setObject(2, obj1);
        pstmt.setObject(3, obj2);
        rs = pstmt.executeQuery();
        List<List<Object>> result2 = MysqlTableCrawlTask.getResultList(rs);
        System.out.println(result2);
        assertTrue(result2.size() == 9);
        
        for(int i=0; i<result2.size(); i++){
            assertTrue(result1.get(i+1).equals(result2.get(i)));
        }
        
        conn.rollback();
        
    }
    
    @Test(timeout=10*60*1000) // timeout 120sec
    public void test_crawl() throws Exception{
        
        Thread.sleep(10000);
        
        final String UNITTEST_SCHEMA = DB_TEST_SCHEMA + "_temp"; 
        
        // copy table adt_test
        Statement stmt = conn.createStatement();
        stmt.execute("DROP DATABASE IF EXISTS " + UNITTEST_SCHEMA);
        stmt.execute("CREATE DATABASE " + UNITTEST_SCHEMA);
        stmt.execute(String.format("CREATE TABLE %s.adt_test_1 LIKE %s.adt_test_1", UNITTEST_SCHEMA, DB_TEST_SCHEMA));
        stmt.execute(String.format("INSERT INTO %s.adt_test_1 SELECT * FROM %s.adt_test_1", UNITTEST_SCHEMA, DB_TEST_SCHEMA));
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + UNITTEST_SCHEMA + ".adt_test_1");
        final List<List<Object>> originalDataList = MysqlTableCrawlTask.getResultList(rs);
        
        
        MysqlCrawlProcessorConfig config = new MysqlCrawlProcessorConfig();
        config.host = DB_HOST;
        config.port = DB_PORT;
        config.user = DB_USER;
        config.password = DB_PW;
        config.jdbcConnParam = "";
        config.dbcp2Properties = new Properties();
        config.tableList = Arrays.asList(UNITTEST_SCHEMA + ".adt_test_1");
        config.selectLimitCount = 100;
        config.selectLockMode = MysqlSelectLockMode.ForUpdate;
        config.concurrentCrawlCount = 1;
        config.workerThreadCountPerTable = 128;
        config.handlerClassName = TestMysqlCrawlProcessorHandler.class.getName();
        
        config.dbcp2Properties.setProperty("maxTotal", "1024");
        config.dbcp2Properties.setProperty("maxIdle", "1024");
        
        final List<List<Object>> crawlDataList = new CopyOnWriteArrayList<>();
        
        final MysqlCrawlProcessor processor = new MysqlCrawlProcessor(config) {
            
            @Override
            public void processData(MysqlCrawlData data) {
                
                if(!data.getTable().getDatabase().getName().equalsIgnoreCase(UNITTEST_SCHEMA)){
                    assertTrue(false);
                }
                if(!data.getTable().getName().equalsIgnoreCase("adt_test_1")){
                    assertTrue(false);
                }
                
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                crawlDataList.addAll(data.getAllRows());
                
            }

            @Override
            public void onException(Exception e) {
                LOGGER.error(e.getMessage(), e);
                stop();
                
            }
        };
        
        processor.start();
        while(!processor.isComplete()){
            Thread.sleep(1000);
            System.out.printf("row count: %d\n", crawlDataList.size());
            System.out.printf("Table Count: %d complete / %d total\n", processor.getCompleteTableCount(), config.tableList.size());
        }

        assertTrue(originalDataList.size() == crawlDataList.size());
        
        
        Comparator<List<Object>> comp = new Comparator<List<Object>>() {

            @Override
            public int compare(List<Object> o1, List<Object> o2) {
                assertTrue(o1.size() == o2.size());
                
                Integer no1 = (Integer) o1.get(0);
                Integer seq1 = (Integer) o1.get(1);
                
                Integer no2 = (Integer) o2.get(0);
                Integer seq2 = (Integer) o2.get(1);
                
                if(no1 == no2){
                    return seq1 - seq2;
                }else{
                    return no1 - no2;
                }
                
            }
        };
        
        Collections.sort(originalDataList, comp);
        Collections.sort(crawlDataList, comp);
        
        for(int i=0; i<originalDataList.size(); i++){
            assertEquals(originalDataList.get(i), crawlDataList.get(i));
        }
        
        stmt.execute("DROP DATABASE IF EXISTS " + UNITTEST_SCHEMA);
        stmt.close();
    }
    
    public void printSqlExecuteResult(List<List<Object>> result){

        for(int i=0; i<result.size(); i++){
            
            System.out.println("==========================");
            
            List<Object> row = result.get(i);
            for(int j=0; j<row.size(); j++){
                Object col = row.get(j);
                System.out.printf("[%d][%d] %s (%s)\n", i, j, col.toString(), col.getClass().getSimpleName());
            }
            
        }
        
    }
    
}
