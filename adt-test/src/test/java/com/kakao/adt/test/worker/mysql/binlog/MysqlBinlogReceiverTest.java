package com.kakao.adt.test.worker.mysql.binlog;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.kakao.adt.mysql.binlog.receiver.MysqlBinlogReceiver;
import com.kakao.adt.mysql.binlog.receiver.MysqlBinlogReceiverEventListener;
import com.kakao.adt.mysql.metadata.MysqlServerInfo;

import static com.kakao.adt.test.TestUtil.*;
import static org.junit.Assert.*;

public class MysqlBinlogReceiverTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogReceiverTest.class);
    
    private MysqlServerInfo serverInfo;
    
    @BeforeClass
    public static void beforeCalss() throws Exception{
        initializeTestEnv();
    }
    
    @Before
    public void before() throws Exception {
        serverInfo = new MysqlServerInfo(DB_HOST, DB_PORT, DB_USER, DB_PW, Arrays.asList(DB_TEST_SCHEMA), true);        
    }
    
    @Test
    public void test_getBinlog() throws Exception{
    
        AtomicLong binlogCnt = new AtomicLong(0L);
        
        MysqlBinlogReceiver receiver = new MysqlBinlogReceiver(
                new HashSet<>(Arrays.asList(DB_TEST_SCHEMA)), 
                new MysqlBinlogReceiverEventListener() {
            
            @Override
            public void onReceivedRowEvent(AbstractRowEvent event) {
                binlogCnt.incrementAndGet();
            }
            
            @Override
            public void onException(MysqlBinlogReceiver receiver, BinlogParser parser,
                    Exception eception) {
                
            }

            @Override
            public void onStop() {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onRotate(RotateEvent event) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onTableMapEvent(TableMapEvent event) {
                // TODO Auto-generated method stub
                
            }
            
        });
        
        receiver.setHost(DB_HOST);
        receiver.setPort(DB_PORT);
        receiver.setUser(DB_USER);
        receiver.setPassword(DB_PW);
        receiver.setServerId(DB_SLAVE_SERVER_ID);
        receiver.setBinlogFileName(serverInfo.getFirstBinaryLogFile().getLogName());
        receiver.start();
        
        long startTime = System.currentTimeMillis();
        for(int i=0; i<10; i++){
            Thread.sleep(1000);
            long curTime = System.currentTimeMillis();
            LOGGER.debug("AVG " + (binlogCnt.get()/(curTime - startTime)) + "/msec");
            LOGGER.debug("TOTAL " + binlogCnt.get());
        }
        receiver.stop(3000, TimeUnit.MILLISECONDS);
        
        LOGGER.debug("received cnt: " + binlogCnt.get());
        assertTrue(binlogCnt.get() > 0);
        
    }
    
}
