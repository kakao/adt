package com.kakao.adt.test.worker;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kakao.adt.parallel.AbstractIOIExecutor;

import static org.junit.Assert.*;

public class ParallelExecutorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelExecutorTest.class);
    
    @Test
    public void ioi_pool_size_1_test() throws Exception{
        testAbstractIOIExecutor(1, 10);
    }
    
    @Test
    public void ioi_pool_size_2_test() throws Exception{
        testAbstractIOIExecutor(2, 20);
    }
    
    @Test
    public void ioi_pool_size_4_test() throws Exception{
        testAbstractIOIExecutor(4, 20);
    }
    
    @Test
    public void ioi_pool_size_8_test() throws Exception{
        testAbstractIOIExecutor(8, 20);
    }
    
    public void testAbstractIOIExecutor(int poolSize, int runtimeInSec) throws Exception{
        
        final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.MINUTES, new LinkedBlockingQueue<>(10000));
        final AbstractIOIExecutor<Long, String> executor = 
            new AbstractIOIExecutor<Long, String>(1000*1000*10, threadPool) {
                private volatile long assertValue = 1;
    
                @Override
                public String process(Long input) {
                    return Long.toString(input);
                }
    
                @Override
                public void completeHandler(String output) {
                    long outputLong = Long.parseLong(output);
                    if(outputLong != assertValue){
                        System.out.printf("Expected: %d, Actual: %d\n", assertValue, outputLong);
                        throw new IllegalStateException("Assertion Fail");
                    }
                    assertValue ++;
                    outputComplete();
                    
                }

                @Override
                public int getMaxWorkerCount() {
                    return this.threadPool.getMaximumPoolSize();
                }
                
                @Override
                public void onException(Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    assertFalse(true);
                }
            };
        
        // INPUT THREAD
        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread inputThread = new Thread(()->{
            long i = 0L;
            try {
                while(running.get()){
                    i++;
                    while(!executor.input(i)){
                        Thread.sleep(1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        inputThread.start();
        
        long startTime = System.currentTimeMillis();
        long exInputCnt = 0;
        long exExecuteCnt = 0;
        long exOutputCnt = 0;
        
        for(int i=0; i<runtimeInSec; i++){
            Thread.sleep(1000);
            long deltaTime = System.currentTimeMillis() - startTime;
            long curInputCnt = executor.getInputCount();
            long curExecuteCnt = executor.getExecuteCount();
            long curOutputCnt = executor.getOutputCount();
            
            System.out.println(String.format("%d, %d, %d", curInputCnt, curExecuteCnt, curOutputCnt));
            System.out.println(String.format(
                    "AVG %d i/msec, %d e/msec %d o/msec", 
                    curInputCnt/deltaTime,
                    curExecuteCnt/deltaTime,
                    curOutputCnt/deltaTime));
            
            assertTrue(exInputCnt <= curInputCnt);
            assertTrue(exExecuteCnt <= curExecuteCnt);
            assertTrue(exOutputCnt <= curOutputCnt);
            
            exInputCnt = curInputCnt;
            exExecuteCnt = curExecuteCnt;
            exOutputCnt = curOutputCnt;
            
        }
        running.set(false);
        inputThread.join();
        System.out.println("Input thread is joined.");
        while(true){
            long curInputCnt = executor.getInputCount();
            long curExecuteCnt = executor.getExecuteCount();
            long curOutputCnt = executor.getOutputCount();
            
            System.out.println(String.format("%d, %d, %d", curInputCnt, curExecuteCnt, curOutputCnt));
            if(curInputCnt == curExecuteCnt && curInputCnt == curOutputCnt){
                break;
            }
            
            Thread.sleep(1000);
            
        }
        
        threadPool.shutdown();
        
    }
    
}
