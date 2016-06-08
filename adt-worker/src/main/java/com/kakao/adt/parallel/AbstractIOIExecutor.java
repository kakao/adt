package com.kakao.adt.parallel;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.*;

/**
 * [I] In order
 * [O] Out of order
 * 
 * [I] Input (only ONE thread call input)
 * [O] Execution 
 * [I] completion callback
 */


public abstract class AbstractIOIExecutor<I, O> implements IParallelExecutor<I, O>, Runnable{
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIOIExecutor.class);
    
    protected final ThreadPoolExecutor threadPool;
    protected final int bufferSize;
    
    private final ArrayList<I> inputs;
    private final ArrayList<O> outputs;
    
    private volatile long inputCnt = 0;
    private volatile long outputCnt = 0;
    private volatile long execCnt = 0;

    private AtomicInteger workerCount = new AtomicInteger(0);
    private ReentrantLock execCountIncrLock = new ReentrantLock();
    private ReentrantLock outputThreadLock = new ReentrantLock();
    
    public AbstractIOIExecutor(
            int bufferSize, 
            ThreadPoolExecutor threadPool){
        this.threadPool = threadPool;
        this.bufferSize = bufferSize;
        
        if(bufferSize < threadPool.getMaximumPoolSize()){
            throw new IllegalArgumentException("Too small buffer size");
        }
        
        this.inputs = new ArrayList<>(bufferSize);
        this.outputs = new ArrayList<>(bufferSize);
        for(int i=0; i<bufferSize; i++){
            this.inputs.add(null);
            this.outputs.add(null);
        }
        
    }
    
    public long getInputCount(){
        return inputCnt;
    }
    
    public long getExecuteCount(){
        return execCnt;
    }
    
    public long getOutputCount(){
        return outputCnt;
    }
    
    public int getMaxWorkerCount(){
        return threadPool.getMaximumPoolSize();
    }
    
    public int getAvailableInputSlot(){
        // inputCnt - outputCnt는 항상 버퍼 사이즈보다 작아야 함
        // 항상 1칸은 예비로 비워둬야 함 (doOutput에서 null체크하는 부분 참조)
        return (int) (bufferSize + outputCnt - inputCnt) - 1; 
                
    }
    
    /** 
     * Thread UNSAFE
     */
    @Override
    public boolean input(I input){
        
        // 버퍼 꽉 차면...
        // (로직 특성 상 약간의 슬롯을 비워놔야 함)
        if(getAvailableInputSlot() <= 0){
            return false;
        }
        
        int bufferIndex = (int)(inputCnt % bufferSize);
        inputs.set(bufferIndex, input);
        inputCnt ++;
        
        scheduleRun();
        
        return true;
        
    }

    private void scheduleRun(){
        
        if(workerCount.get() < threadPool.getMaximumPoolSize()){
            workerCount.incrementAndGet();
            threadPool.execute(this);
        }
        
    }
    
    @Override
    public void run() {
        try{
            while(true){
                
                long curExecCnt;
                long curInputCnt = inputCnt;
                
                execCountIncrLock.lock();
                // 더 이상 처리할 게 없으면 curExecCnt == -1
                // 아직 처리할 게 남아있으면 execCnt ==> curExecCnt 대입 후 1 증가
                curExecCnt = (execCnt >= curInputCnt ? -1 : execCnt ++); 
                execCountIncrLock.unlock();
                
                // 더 이상 처리할 게 없을 경우
                if(curExecCnt == -1){
                
                    // 일단 job 중지
                    workerCount.decrementAndGet();
                    
                    // 그 사이에 input 개수 변화가 있으면 reschedule
                    if(curInputCnt != inputCnt){
                        scheduleRun();
                    }
                        
                    break;
                }
                
                final int bufferIndex = (int)(curExecCnt % bufferSize);
                final I input = inputs.get(bufferIndex);
                O output = process(input);
                outputs.set(bufferIndex, output);
                
                doOutput();
                
            }
        }catch(Exception e){
            onException(e);
        }
                    
    }
    
    private void doOutput(){
        while(true){
            int bufferIndex = -1;
            
            while(true){
                if(!outputThreadLock.tryLock()){
                    return;
                }
                
                bufferIndex = (int)(outputCnt % bufferSize);
                O output = outputs.get(bufferIndex);
                if(output == null){
                    //LOGGER.debug("stopped at " + outputCnt);
                    outputThreadLock.unlock();
                    break;
                }
                
                // completeHandler may call outputComplete(), 
                // so outputThreadLock will be released.
                completeHandler(output);
                
            }
            
            // outputThreadLock release하는 동안
            // 새로운 output이 추가되었으면 재시도
            if(outputs.get(bufferIndex) != null){
                continue;
            }
            
            // 더 이상 output할 게 없으면
            // 종료하고 run()으로 돌아가서 새로운 input 처리
            else{
                break;
            }
        }

        
    }
    
    /**
     * Thread UNSAFE
     * This method should be called in completeHandler
     * 
     */
    public void outputComplete(){
        
        final int bufferIndex = (int)(outputCnt % bufferSize);
        inputs.set(bufferIndex, null);
        outputs.set(bufferIndex, null);
        outputCnt ++;
        
        outputThreadLock.unlock();
    }
    
}