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

public class MysqlBinlogProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogProcessorTest.class);

    @BeforeClass
    public static void beforeClass() throws Exception{
        initializeTestEnv();
        
        final int sleepCount = 30;
        
        LOGGER.debug("sleep " + sleepCount + "sec for generating enough test data");
        for(int i=1; i<=sleepCount; i++){
            LOGGER.debug(i + "/" + sleepCount);
            Thread.sleep(1000);
        }
    }
    
    @Test
    public void test_RowEventProcessor() throws Exception{
        
        final MysqlServerInfo serverInfo = new MysqlServerInfo(DB_HOST, DB_PORT, DB_USER, DB_PW, Collections.emptyList(), true);
        final String startLogFileName = serverInfo.getBinaryLogFileList().get(0).getLogName();
        
        final MysqlBinlogProcessorConfig config = new MysqlBinlogProcessorConfig();
        config.host = DB_HOST;
        config.port = DB_PORT;
        config.user = DB_USER;
        config.password = DB_PW;
        config.databaseList = Arrays.asList(DB_TEST_SCHEMA);
        config.mysqlSlaveServerId = DB_SLAVE_SERVER_ID;
        config.binlogFileName = startLogFileName;
        config.binlogFilePosition = 4;
        config.taskQueueCount = 256;
        config.eventBufferSize = 1000*1000;
        config.workerThreadCorePoolSize = 4;
        config.handlerClassName = TestMysqlBinlogProcessorHandler.class.getName();
        
        final int runtimeInSec = 30;
        final AtomicLong accTaskProcCount = new AtomicLong(0);
        
        MysqlBinlogProcessor processor = new MysqlBinlogProcessor(config) {
            
            @Override
            public void processData(MysqlBinlogData data) {
                accTaskProcCount.incrementAndGet();
            }

            @Override
            public int getMaxWorkerCount() {
                return this.threadPool.getMaximumPoolSize();
            }
        };
        processor.start();
        
        long startTime = System.currentTimeMillis();
        long exInputCnt = 0;
        long exExecuteCnt = 0;
        long exOutputCnt = 0;
        long exTaskProcCnt = 0;
        
        for(int i=0; i<runtimeInSec; i++){
            Thread.sleep(1000);
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
        
        processor.stop();
           
    }
    
}
