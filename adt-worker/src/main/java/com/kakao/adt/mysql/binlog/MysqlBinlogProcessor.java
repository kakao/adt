package com.kakao.adt.mysql.binlog;

import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.kakao.adt.IProcessor;
import com.kakao.adt.handler.MysqlBinlogProcessorHandler;
import com.kakao.adt.misc.LongWrapper;
import com.kakao.adt.misc.Tuple2;
import com.kakao.adt.mysql.binlog.receiver.MysqlBinlogReceiver;
import com.kakao.adt.mysql.binlog.receiver.MysqlBinlogReceiverEventListener;
import com.kakao.adt.mysql.metadata.MysqlServerInfo;
import com.kakao.adt.parallel.AbstractIOIExecutor;

public class MysqlBinlogProcessor 
            extends AbstractIOIExecutor<Tuple2<AbstractRowEvent, TableMapEvent>, List<MysqlBinlogTask>> 
            implements MysqlBinlogReceiverEventListener, IProcessor<MysqlBinlogData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogProcessor.class);
    private static final int STATUS_NOT_STARTED = 0;
    private static final int STATUS_STARTING = 1;
    private static final int STATUS_RUNNING = 2;
    private static final int STATUS_STOPPING = 3;
    private static final int STATUS_STOPPED = 4;
    
    private final MysqlBinlogProcessorConfig config;
    private final MysqlBinlogReceiver binlogReceiver;
    private final MysqlServerInfo serverInfo;
    private final Set<String> acceptableDatabaseSet;
    private final ArrayList<MysqlBinlogTaskQueue> taskQueueList;
    private final MysqlBinlogProcessorHandler handler;
    
    private final CountDownLatch waitStopLatch = new CountDownLatch(1);
    private final AtomicReference<Exception> execException = new AtomicReference<Exception>(null);
    
    private final Map<Long, TableMapEvent> tableMapEventMapper = new HashMap<>();
    private final ConcurrentHashMap<String, LongWrapper> binlogFileInputCount = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> binlogFileCompleteCount = new ConcurrentHashMap<>();
    
    private final AtomicInteger status = new AtomicInteger(STATUS_NOT_STARTED);
    
    public MysqlBinlogProcessor(MysqlBinlogProcessorConfig _config) throws Exception {
        
        super(_config.eventBufferSize, 
            new ThreadPoolExecutor(
                _config.workerThreadCorePoolSize, 
                _config.workerThreadCorePoolSize, 
                10, 
                TimeUnit.MINUTES, 
                new LinkedBlockingDeque<>()));
        
        _config.checkValidation();
        
        this.config = _config;
        this.serverInfo = new MysqlServerInfo(config.host, config.port, config.user, config.password, config.databaseList);
        this.binlogReceiver = new MysqlBinlogReceiver(new HashSet<>(config.databaseList), this);
        this.binlogReceiver.setHost(config.host);
        this.binlogReceiver.setPort(config.port);
        this.binlogReceiver.setUser(config.user);
        this.binlogReceiver.setPassword(config.password);
        this.binlogReceiver.setServerId(config.mysqlSlaveServerId);
        this.binlogReceiver.setBinlogFileName(config.binlogFileName);
        this.binlogReceiver.setBinlogPosition(config.binlogFilePosition);
        
        this.acceptableDatabaseSet = new HashSet<>(config.databaseList);
        LOGGER.info("ACCEPT DB LIST: " + acceptableDatabaseSet);
        
        this.taskQueueList = new ArrayList<>(config.taskQueueCount);
        for(int i=0; i<config.taskQueueCount; i++){
            taskQueueList.add(new MysqlBinlogTaskQueue(this, i));
        }

        // init handler
        @SuppressWarnings("unchecked")
        final Class<? extends MysqlBinlogProcessorHandler> handlerClass = 
                (Class<? extends MysqlBinlogProcessorHandler>) Class.forName(config.handlerClassName);
        handler = handlerClass.newInstance();
        if(!(handler instanceof MysqlBinlogProcessorHandler)){
            throw new IllegalArgumentException("Class [" + config.handlerClassName + "] is invalid.");
        }
        
    }
    
    public MysqlServerInfo getMysqlServerInfo(){
        return serverInfo;
    }
    
    public void start() throws Exception{
        if(!status.compareAndSet(STATUS_NOT_STARTED, STATUS_STARTING)){
            LOGGER.error("status should be NOT_STARTED. status=" + status.get());
            return;
        }
        getHandler().beforeStart();
        createBinlogFileStatCounter(config.binlogFileName);
        binlogReceiver.start();
        
        status.set(STATUS_RUNNING);
    }
    
    /**
     * this method doesn't guarantee graceful stop
     */
    public void stop(){
        if(!status.compareAndSet(STATUS_RUNNING, STATUS_STOPPING)){
            if(status.get() == STATUS_STOPPING){
                LOGGER.warn("processor is already stopping, now");
            }else{
                LOGGER.error("status should be RUNNING. status=" + status.get());
            }
            return;
        }
        
        try{
            binlogReceiver.stop(config.binlogReceiverStopTimeout, TimeUnit.MILLISECONDS);
        }catch(Exception e){
            LOGGER.error("Failed to stop binlogReceiver.", e);
        }
        
        LOGGER.info("try to shutdown threadPool");
        threadPool.shutdownNow();
        
        try {
            threadPool.awaitTermination(config.threadPoolTerminationTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.error("Failed to await thread pool termination.", e);
        }
        status.set(STATUS_STOPPED);
        
        logRemainedBinlogFileStatCounter();
        
        getHandler().afterStop();
        
        waitStopLatch.countDown();
        
    }
    
    @Override
    public void waitStop() throws Exception {
        waitStopLatch.await();
        
        if(execException.get() != null){
            throw new RuntimeException(execException.get());
        }
    }
    
    @Override
    public MysqlBinlogProcessorHandler getHandler() {
        return this.handler;
    }
    
    public void createBinlogFileStatCounter(String binlogFileName){
        
        binlogFileInputCount.put(binlogFileName, new LongWrapper());
        binlogFileCompleteCount.put(binlogFileName, new AtomicLong());
    
        final List<Entry<String, LongWrapper>> entryList = new ArrayList<>(binlogFileInputCount.entrySet());
        entryList.sort(new Comparator<Entry<String, LongWrapper>>() {
            @Override
            public int compare(Entry<String, LongWrapper> o1, Entry<String, LongWrapper> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        
        // remove complete files in list
        for(int i=0; i<entryList.size() - 1; i++){
            Entry<String, LongWrapper> entry = entryList.get(i);
            final String fileName = entry.getKey();
            final long inputCount = entry.getValue().getValue();
            
            if(inputCount == binlogFileCompleteCount.get(fileName).get()){
                LOGGER.info("[BINLOG FILE PROGRESS] [" + fileName + "] " + inputCount + " / " + inputCount + " (complete)");
                binlogFileInputCount.remove(fileName);
                binlogFileCompleteCount.remove(fileName);
            }else{
                break;
            }
            
        }
        
        logRemainedBinlogFileStatCounter();
    }
    
    public void logRemainedBinlogFileStatCounter(){
        while(true){    
            try{
                final Map<String, LongWrapper> inputCopied = new TreeMap<>(binlogFileInputCount);
                for(Entry<String, LongWrapper> entry : inputCopied.entrySet()){
                    final String file = entry.getKey();
                    LOGGER.info("[BINLOG FILE PROGRESS] [" + entry.getKey() + "] " + 
                            binlogFileCompleteCount.get(file).get() + " / " + entry.getValue().getValue());
                }
                break;
            }catch(Exception e){
                LOGGER.warn(e.getMessage(), e);
            }
        }
    }
    
    //==============================================================
    //
    //      Impl. of MysqlBinlogReceiverEventListener
    //
    //==============================================================
    
    
    @Override
    public void onReceivedRowEvent(AbstractRowEvent event) {
        
        final int rowCount = MysqlBinlogHandler.valueOf(event).getRowCount(event);
        final LongWrapper inputCount = binlogFileInputCount.get(event.getBinlogFilename());
        inputCount.add(rowCount);
        
        final Tuple2<AbstractRowEvent, TableMapEvent> inputValue = 
                new Tuple2<AbstractRowEvent, TableMapEvent>(event, tableMapEventMapper.get(event.getTableId()));
        while(!this.input(inputValue)){
            try {
                Thread.sleep(config.eventBufferInputFailSleepTime);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }
    
    
    @Override
    public void onTableMapEvent(TableMapEvent event) {
        tableMapEventMapper.put(event.getTableId(), event);
    }
    
    @Override
    public void onRotate(RotateEvent event) {
        
        final String currentFileName = event.getBinlogFilename();
        final String nextFileName = event.getBinlogFileName().toString();
        
        if(!currentFileName.equals(nextFileName)){
            
            final long timestamp = event.getHeader().getTimestamp();
            final Instant time = Instant.ofEpochMilli(timestamp);
            LOGGER.info("ROTATE: " + currentFileName + " -> " + nextFileName + 
                    " timestamp=" + event.getHeader().getTimestamp() + 
                    " " + OffsetDateTime.ofInstant(time, ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            createBinlogFileStatCounter(nextFileName);
        }
        
    }
    
    @Override
    public void onException(
            MysqlBinlogReceiver receiver, 
            BinlogParser parser, 
            Exception exception) {
        
        onException(exception);
        
    }
    
    @Override
    public void onException(Exception exception){
        LOGGER.error(exception.getMessage(), exception);
        execException.compareAndSet(null, exception);
        stop();
    }
    
    /**
     * if binlog receiver is stopped, this method will be called.
     */
    @Override
    public void onStop() {
        // if binlog processor is still running, then...
        if(status.get() != STATUS_STOPPING){
            onException(new IllegalStateException("Binlog Receiver is stopped ungracefully."));
        }
    }
    
    //==============================================================
    //
    //      Impl. of AbstractIOIExecutor
    //
    //==============================================================
    
    @Override
    public List<MysqlBinlogTask> process(Tuple2<AbstractRowEvent, TableMapEvent> input) {
        final AbstractRowEvent event = input.getA();
        final TableMapEvent tableMapEvent = input.getB();
        
        if(!acceptableDatabaseSet.contains(tableMapEvent.getDatabaseName().toString())){
            final String binlogFileName = event.getBinlogFilename();
            final AtomicLong completeCount = binlogFileCompleteCount.get(binlogFileName);
            completeCount.incrementAndGet();
            
            return Collections.emptyList();
        }
        
        List<MysqlBinlogTask> rowEventList = MysqlBinlogHandler.valueOf(event).generateMysqlBinlogRowEventTask(this, event, tableMapEvent);
        
        for(MysqlBinlogTask rowEvent: rowEventList){
            rowEvent.setQueueListSizeAndCalcQueueIndex(taskQueueList.size());
        }
        
        return rowEventList;
        
    }

    @Override
    public void completeHandler(List<MysqlBinlogTask> output) {
        
        for(MysqlBinlogTask task : output){
            Set<Integer> queueIndexes = task.getQueueIndexes();
            for(Integer queueIndex : queueIndexes){
                MysqlBinlogTaskQueue queue = taskQueueList.get(queueIndex);
                if(queue.size() >= config.taskQueueMaxCount){
                    queue.run();
                    while(queue.size() >= config.taskQueueMaxCount){
                        try{
                            Thread.sleep(config.taskQueueInputFailSleepTime);
                        }catch(Exception e){
                            throw new IllegalStateException(e);
                        }
                    }
                }
                queue.add(task);
            }
        }
        
        outputComplete();
        
        Set<Integer> procQueueIndexes = new HashSet<>();
        for(MysqlBinlogTask task : output){
            Set<Integer> queueIndexes = task.getQueueIndexes();
            procQueueIndexes.addAll(queueIndexes);
        }
        
        for(final Integer queueIndex : procQueueIndexes){
            MysqlBinlogTaskQueue queue = taskQueueList.get(queueIndex);
            queue.run();
        }
        
    }

    public void processQueue(int queueIndex){
        MysqlBinlogTaskQueue queue = taskQueueList.get(queueIndex);
        if(status.get() == STATUS_RUNNING || status.get() == STATUS_STARTING){
            threadPool.execute(queue);
        }
    }
    
    public void processData(MysqlBinlogTask task){
        try {
            processData(task.asBinlogData());
            
            final String binlogFileName = task.getBinlogFileName();
            final AtomicLong completeCount = binlogFileCompleteCount.get(binlogFileName);
            completeCount.incrementAndGet();
            
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            onException(e);
        }
    }
    
    @Override
    public void processData(MysqlBinlogData data) throws Exception{
        getHandler().processData(data);
    }
    
    public ThreadPoolExecutor getWorkerThreadPoolForTest(){
        return this.threadPool;
    }
}
