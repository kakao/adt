package com.kakao.adt.test.worker.mysql.binlog;

import static com.kakao.adt.test.TestUtil.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.kakao.adt.mysql.binlog.receiver.MysqlBinlogReceiver;
import com.kakao.adt.mysql.binlog.receiver.MysqlBinlogReceiverEventListener;
import com.kakao.adt.mysql.metadata.MysqlServerInfo;

public class MysqlBinlogHandlerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogHandlerTest.class);
    
    private MysqlServerInfo serverInfo;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        
        initializeTestEnv();
        
    }
    
    @Before
    public void before() throws Exception {
        serverInfo = new MysqlServerInfo(
                DB_HOST, 
                DB_PORT, 
                DB_USER, 
                DB_PW, 
                Arrays.asList(DB_TEST_SCHEMA),
                true);        
    }

    @Test
    public void test_receiveHandler() throws Exception{
        MysqlBinlogReceiverForTest receiver = new MysqlBinlogReceiverForTest(
                new HashSet<>(Arrays.asList(DB_TEST_SCHEMA)), 
                new MysqlBinlogReceiverEventListener() {
            
            @Override
            public void onReceivedRowEvent(AbstractRowEvent event) {
                // NOTHING TO DO
            }
            
            @Override
            public void onException(MysqlBinlogReceiver receiver, BinlogParser parser,
                    Exception eception) {
                // NOTHING TO DO
            }

            @Override
            public void onStop() {
                // NOTHING TO DO
            }

            @Override
            public void onRotate(RotateEvent event) {
                // NOTHING TO DO
            }

            @Override
            public void onTableMapEvent(TableMapEvent event) {
                // NOTHING TO DO
            }
            
        });
        
        receiver.setHost(DB_HOST);
        receiver.setPort(DB_PORT);
        receiver.setUser(DB_USER);
        receiver.setPassword(DB_PW);
        receiver.setServerId(DB_SLAVE_SERVER_ID);
        receiver.setBinlogFileName(serverInfo.getFirstBinaryLogFile().getLogName());
        
        receiver.start();
        for(int i=0; i<600; i++){
            Thread.sleep(1000);
            LOGGER.debug("rotateEventCnt: " + receiver.rotateCount);
            LOGGER.debug("rowEventCnt: " + receiver.rowEventCount);
            LOGGER.debug("tableMapEventCnt: " + receiver.tableMapEventCount);
            
            if(receiver.rotateCount > 0){
                break;
            }
        }
        
        receiver.stop(3000, TimeUnit.MILLISECONDS);
        
        assertTrue(receiver.rotateCount > 0);
        assertTrue(receiver.rowEventCount > 0);
        assertTrue(receiver.tableMapEventCount > 0);
    }
    
    private static class MysqlBinlogReceiverForTest extends MysqlBinlogReceiver{

        public volatile long rotateCount = 0;
        public volatile long rowEventCount = 0;
        public volatile long tableMapEventCount = 0;
        
        public MysqlBinlogReceiverForTest(Set<String> acceptableDatabases,
                MysqlBinlogReceiverEventListener receiverEventListener) {
            super(acceptableDatabases, receiverEventListener);
            
        }
        
        @Override
        public void handleRotateEvent(RotateEvent event) {
            super.handleRotateEvent(event);
            rotateCount ++;
        }
        
        @Override
        public void handleRowEvent(AbstractRowEvent event) {
            super.handleRowEvent(event);
            rowEventCount ++;
        }
        
        @Override
        public void handleTableMapEvent(TableMapEvent event) {
            super.handleTableMapEvent(event);
            tableMapEventCount ++;
        }
        
    }
    

}

