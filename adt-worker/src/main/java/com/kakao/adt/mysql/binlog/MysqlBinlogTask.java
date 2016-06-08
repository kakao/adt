package com.kakao.adt.mysql.binlog;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.glossary.Row;
import com.kakao.adt.misc.BooleanWrapper;
import com.kakao.adt.mysql.metadata.Index;
import com.kakao.adt.mysql.metadata.MysqlServerInfo;
import com.kakao.adt.mysql.metadata.Table;

public class MysqlBinlogTask {

    public static final int STATUS_WAIT = 0;
    public static final int STATUS_RUNNABLE = 1;
    public static final int STATUS_SKIP = 2;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogTask.class);
    
    private MysqlBinlogProcessor processor;
    private TableMapEvent tableMapEvent;
    private MysqlBinlogHandler type;
    private Row oldRow;
    private Row newRow;
    
    private String binlogFileName;
    private long binlogPosition;
    private long binlogTimestamp;
    private int binlogCurrentRowNumber;
    private int binlogTotalRowNumber;
    
    private Set<String> dependencyValues = new HashSet<>();
    // concurrently readable    
    private Map<Integer, BooleanWrapper> waitingFlag = new HashMap<>(); // key: queue idx, value: boolean
    // concurrently writable
    private AtomicInteger waitingCount = new AtomicInteger(0);
    // concurrently readable
    private volatile boolean _isFinished = false; 
    
    public static MysqlBinlogTask getInstance(
            final MysqlBinlogProcessor processor,
            final TableMapEvent tableMapEvent,
            final MysqlBinlogHandler type, 
            final Row oldRow,
            final Row newRow,
            final String binlogFileName,
            final long binlogPosition,
            final long binlogTimestamp,
            final int binlogCurrentRowNumber,
            final int binlogTotalRowNumber){
        
        MysqlBinlogTask task = new MysqlBinlogTask();
        task.initialize(
                processor, 
                tableMapEvent, 
                type, 
                oldRow, 
                newRow, 
                binlogFileName, 
                binlogPosition, 
                binlogTimestamp,
                binlogCurrentRowNumber,
                binlogTotalRowNumber);
        
        return task;
        
    }
    
    private MysqlBinlogTask() { }

    //===========================================
    //
    //      INITIALIZE
    //
    //===========================================
    
    private void initialize(
            final MysqlBinlogProcessor processor,
            final TableMapEvent tableMapEvent,
            final MysqlBinlogHandler type, 
            final Row oldRow,
            final Row newRow,
            final String binlogFileName,
            final long binlogPosition,
            final long binlogTimestamp,
            final int binlogCurrentRowNumber,
            final int binlogTotalRowNumber){
        
        this.processor = processor;
        this.tableMapEvent = tableMapEvent;
        this.type = type;
        this.oldRow = oldRow;
        this.newRow = newRow;
        
        this.binlogFileName = binlogFileName;
        this.binlogPosition = binlogPosition;
        this.binlogTimestamp = binlogTimestamp;
        this.binlogCurrentRowNumber = binlogCurrentRowNumber;
        this.binlogTotalRowNumber = binlogTotalRowNumber;
        
        dependencyValues.clear();
        waitingFlag.clear();
        waitingCount.set(0);
        _isFinished = false;
        
        // gen idx value
        calcDepedencyValues();
    }
    
    private void calcDepedencyValues(){
        
        MysqlServerInfo serverInfo = processor.getMysqlServerInfo();
        final String dbName = tableMapEvent.getDatabaseName().toString();
        final String tableName = tableMapEvent.getTableName().toString();
        
        final List<Index> indexes = serverInfo.getDatabase(dbName).getTable(tableName).getIndexes();
        for(final Index idx : indexes){
             
            if(!idx.isUnique()){ // skip if not unique
                continue;
            }
            
            if(oldRow != null){
                calcDependencyValue(dbName, tableName, oldRow, idx);
            }
            
            if(newRow != null){
                calcDependencyValue(dbName, tableName, newRow, idx);
            }
        }
        
    }
    
    private void calcDependencyValue(String dbName, String tableName, Row row, Index idx){
        
        final List<com.google.code.or.common.glossary.Column> columnList = row.getColumns();
        
        StringBuilder builder = new StringBuilder(dbName)
                .append('.').append(tableName)
                .append('.').append(idx.getName());
        
        for(int i=0; i<idx.getColumns().size(); i++){
            
            builder.append('[').append(idx.getColumn(i).getName()).append('=');
            com.google.code.or.common.glossary.Column col = columnList.get(idx.getColumn(i).getPosition());
            Object value = col.getValue();
            
            if(value == null){
                
            }else if(value instanceof byte[]){
                Arrays.toString((byte[])value);
            }else{
                builder.append(value.toString());
            }
            builder.append("]");
        }
        
        dependencyValues.add(builder.toString());
        
    }

    public void setQueueListSizeAndCalcQueueIndex(int len){
        Set<Integer> queueIndexes = new HashSet<>();
        for(String depValue : dependencyValues){
            queueIndexes.add(Math.abs(depValue.hashCode()) % len);
        }
        
        for(Integer queueIdx : queueIndexes){
            waitingFlag.put(queueIdx, new BooleanWrapper(false));
        }
        
    }
    
    public Set<Integer> getQueueIndexes(){
        return waitingFlag.keySet();
    }
    
    public TableMapEvent getTableMapEvent(){
        return this.tableMapEvent;
    }

    public Table getTable(){
        return processor.getMysqlServerInfo()
                .getDatabase(tableMapEvent.getDatabaseName().toString())
                .getTable(tableMapEvent.getTableName().toString());
    }
    
    public MysqlBinlogHandler getMysqlBinlogType(){
        return this.type;
    }
    
    public Row getRowBefore(){
        return oldRow;
    }
    
    public Row getRowAfter(){
        return newRow;
    }
    
    public MysqlBinlogData asBinlogData(){
        return new MysqlBinlogData(getTable(), type, oldRow, newRow);
    }
    
    /** 
     * Called by multiple threads.
     * @param queueIdx
     * @return
     */
    public RowEventTaskStatus checkRunnable(int queueIdx){
        boolean incrFirstElemCnt = waitingFlag.get(queueIdx).compareAndSet(false, true);
        // first try
        if(incrFirstElemCnt){
            int curCnt = waitingCount.incrementAndGet();
            // 첫 액세스한 스레드 중 가장 마지막에 접근한 녀석이 처리한다.
            if(curCnt == waitingFlag.size()){
                return RowEventTaskStatus.RUNNABLE;
            }
            // 그 외에는 대기
            else{
                return RowEventTaskStatus.WAITING;
            }
        }
        // retry
        else{
            // if already finished
            if(_isFinished){
                return RowEventTaskStatus.ALREADY_DONE;
            }
            else{
                return RowEventTaskStatus.WAITING;
            }
        }
    }
    
    public void setFinished(){
        this._isFinished = true;
    }
    
    public boolean isFinished(){
        return this._isFinished;
    }
    
    public static enum RowEventTaskStatus{
        RUNNABLE,
        WAITING,
        ALREADY_DONE,
        
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public long getBinlogTimestamp() {
        return binlogTimestamp;
    }

    public int getBinlogCurrentRowNumber() {
        return binlogCurrentRowNumber;
    }

    public int getBinlogTotalRowNumber() {
        return binlogTotalRowNumber;
    }
    
}
