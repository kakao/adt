package com.kakao.adt.mysql.binlog.receiver;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventFilter;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.BinlogParserListener;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.kakao.adt.mysql.binlog.MysqlBinlogHandler;

/**
 * @see https://github.com/zendesk/open-replicator
 * @author gordon
 *
 */
public class MysqlBinlogReceiver extends OpenReplicator 
        implements 
        BinlogEventListener, 
        BinlogEventFilter,
        BinlogParserListener{

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogReceiver.class);
    
    private final MysqlBinlogReceiverEventListener receiverEventListener;
    
    public MysqlBinlogReceiver(
            Set<String> acceptableDatabases, 
            MysqlBinlogReceiverEventListener receiverEventListener) {
        
        super();
        this.receiverEventListener = receiverEventListener;
        this.setBinlogEventListener(this);
        
    }

    @Override
    public void start() throws Exception {
        
        super.start();
        this.binlogParser.setEventFilter(this);
        this.binlogParser.addParserListener(this);
        
    }
    
    @Override
    public void onStart(BinlogParser parser) {
        LOGGER.info("Binlog Receiver is started.");
        LOGGER.info("parser=" + parser);
    }


    @Override
    public void onStop(BinlogParser parser) {
        
        LOGGER.info("Binlog Receiver is stopped.");
        LOGGER.info("parser=" + parser);
        
        receiverEventListener.onStop();
        
    }


    @Override
    public void onException(BinlogParser parser, Exception eception) {
        receiverEventListener.onException(this, parser, eception);
    }


    @Override
    public boolean accepts(BinlogEventV4Header header,
            BinlogParserContext context) {
        
        final MysqlBinlogHandler properHandler = MysqlBinlogHandler.valueOf(header.getEventType());
        if( MysqlBinlogHandler.NOT_IMPLEMENTED_EVENT_HANDLER == properHandler) {
            return false;
        }
        
        return true;
        
    }

    @Override
    public void onEvents(BinlogEventV4 event) {
        try{
            MysqlBinlogHandler.valueOf(event).mysqlBinlogReceiverOnEventsHandler(this, event);
        }catch(Exception e){
            LOGGER.error(e.getMessage(), e);
            try {
                this.stop(10, TimeUnit.SECONDS);
            } catch (Exception e1) {
                LOGGER.error(e1.getMessage(), e1);
            }
        }
        
    }  
    
    public void handleTableMapEvent(TableMapEvent event){
        receiverEventListener.onTableMapEvent(event);
    }
    
    public void handleRotateEvent(RotateEvent event){
        receiverEventListener.onRotate(event);
    }
    
    public void handleRowEvent(AbstractRowEvent event){
        receiverEventListener.onReceivedRowEvent(event);
    }
    
}
