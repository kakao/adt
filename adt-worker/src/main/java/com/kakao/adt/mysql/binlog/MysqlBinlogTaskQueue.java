package com.kakao.adt.mysql.binlog;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kakao.adt.mysql.binlog.MysqlBinlogTask.RowEventTaskStatus;

public class MysqlBinlogTaskQueue extends ConcurrentLinkedQueue<MysqlBinlogTask> implements Runnable{

    private static final long serialVersionUID = -6837313003030705397L;
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogTaskQueue.class);
    
    private final AtomicBoolean runFlag = new AtomicBoolean(false);
    private final int queueIndex;
    private final MysqlBinlogProcessor processor;
    
    public MysqlBinlogTaskQueue(MysqlBinlogProcessor processor, int queueIndex) {
        super();
        this.processor = processor;
        this.queueIndex = queueIndex;
    }
    
    @Override
    public void run() {
        
        try{
        
            while(true){
                if(!runFlag.compareAndSet(false, true)){
                    break;
                }
                
                boolean isEmpty = false;
             
                while(true){
                    
                    MysqlBinlogTask task = this.peek();
                    
                    // if there's no remained task, then...
                    if(task == null){
                        isEmpty = true;
                        break;
                    }
                    
                    RowEventTaskStatus taskStatus = task.checkRunnable(queueIndex);
                    if(taskStatus == RowEventTaskStatus.RUNNABLE){
                        this.poll();
                    }else if(taskStatus == RowEventTaskStatus.WAITING){
                        break;
                    }else if(taskStatus == RowEventTaskStatus.ALREADY_DONE){
                        this.poll();
                        continue;
                    }
                    
                    // handle current task
                    processor.processData(task);
                    
                    // after processing a task...
                    task.setFinished();
                    for(final Integer queueIndex : task.getQueueIndexes()){
                        if(queueIndex == this.queueIndex){
                            continue;
                        }
                        processor.processQueue(queueIndex);
                    }
                    
                }
                
                // stop
                runFlag.set(false);
                
                // check one more time before exit this method
                // (check if new element exists 
                // that inserted before calling [runFlag.set(false)] )
                if(isEmpty && this.peek() != null){
                    continue;
                }
                
                break;
                
            }
        }catch(Exception e){
            processor.onException(e);
        }
        
    }

}
