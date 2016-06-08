package com.kakao.adt.mysql.binlog;

import static com.google.code.or.common.util.MySQLConstants.DELETE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.DELETE_ROWS_EVENT_V2;
import static com.google.code.or.common.util.MySQLConstants.ROTATE_EVENT;
import static com.google.code.or.common.util.MySQLConstants.TABLE_MAP_EVENT;
import static com.google.code.or.common.util.MySQLConstants.UPDATE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.UPDATE_ROWS_EVENT_V2;
import static com.google.code.or.common.util.MySQLConstants.WRITE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.WRITE_ROWS_EVENT_V2;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.kakao.adt.mysql.binlog.receiver.MysqlBinlogReceiver;

public enum MysqlBinlogHandler {

    TABLE_MAP_EVENT_HANDLER(TABLE_MAP_EVENT, false) {

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            receiver.handleTableMapEvent((TableMapEvent) event);
            
        }

        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent event,
                TableMapEvent tableMapEvent) {
            return null;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return 0;
        }
    },
    WRITE_ROWS_EVENT_HANDLER(WRITE_ROWS_EVENT, true) {

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            receiver.handleRowEvent((AbstractRowEvent) event);
            
        }

        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent _event,
                TableMapEvent tableMapEvent) {
            
            WriteRowsEvent event = (WriteRowsEvent) _event;
            final List<Row> rowList = event.getRows();
            final List<MysqlBinlogTask> rowEventList = new ArrayList<>(rowList.size());
            
            for(int i=0; i<rowList.size(); i++){
                final Row newRow = rowList.get(i);
                MysqlBinlogTask rowEvent = MysqlBinlogTask.getInstance(
                        processor, 
                        tableMapEvent, 
                        this, 
                        null,
                        newRow,
                        event.getBinlogFilename(),
                        event.getHeader().getPosition(),
                        event.getHeader().getTimestamp(),
                        i,
                        rowList.size());
                rowEventList.add(rowEvent);
            }
            return rowEventList;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return ((WriteRowsEvent) event).getRows().size();
        }
    },
    WRITE_ROWS_EVENT_V2_HANDLER(WRITE_ROWS_EVENT_V2, true) {

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            receiver.handleRowEvent((AbstractRowEvent) event);            
        }

        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent _event,
                TableMapEvent tableMapEvent) {
            
            WriteRowsEventV2 event = (WriteRowsEventV2) _event;
            final List<Row> rowList = event.getRows();
            final List<MysqlBinlogTask> rowEventList = new ArrayList<>(rowList.size());
            
            for(int i=0; i<rowList.size(); i++){
                final Row newRow = rowList.get(i);
                MysqlBinlogTask rowEvent = MysqlBinlogTask.getInstance(
                        processor, 
                        tableMapEvent, 
                        this, 
                        null,
                        newRow,
                        event.getBinlogFilename(),
                        event.getHeader().getPosition(),
                        event.getHeader().getTimestamp(),
                        i,
                        rowList.size());
                rowEventList.add(rowEvent);
            }
            return rowEventList;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return ((WriteRowsEventV2) event).getRows().size();
        }
    },
    UPDATE_ROWS_EVENT_HANDLER(UPDATE_ROWS_EVENT, true) {

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            receiver.handleRowEvent((AbstractRowEvent) event);            
        }

        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent _event,
                TableMapEvent tableMapEvent) {
            
            UpdateRowsEvent event = (UpdateRowsEvent) _event;
            final List<Pair<Row>> pairList = event.getRows();
            final List<MysqlBinlogTask> rowEventList = new ArrayList<>(pairList.size());
            
            for(int i=0; i<pairList.size(); i++){
                final Pair<Row> pair = pairList.get(i);
                MysqlBinlogTask rowEvent = MysqlBinlogTask.getInstance(
                        processor, 
                        tableMapEvent, 
                        this, 
                        pair.getBefore(), 
                        pair.getAfter(),
                        event.getBinlogFilename(),
                        event.getHeader().getPosition(),
                        event.getHeader().getTimestamp(),
                        i,
                        pairList.size());
                rowEventList.add(rowEvent);
            }
            return rowEventList;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return ((UpdateRowsEvent) event).getRows().size();
        }
    },
    UPDATE_ROWS_EVENT_V2_HANDLER(UPDATE_ROWS_EVENT_V2, true) {

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            receiver.handleRowEvent((AbstractRowEvent) event);            
        }

        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent _event,
                TableMapEvent tableMapEvent) {
            
            UpdateRowsEventV2 event = (UpdateRowsEventV2) _event;
            final List<Pair<Row>> pairList = event.getRows();
            final List<MysqlBinlogTask> rowEventList = new ArrayList<>(pairList.size());
            
            for(int i=0; i<pairList.size(); i++){
                final Pair<Row> pair = pairList.get(i);
                MysqlBinlogTask rowEvent = MysqlBinlogTask.getInstance(
                        processor, 
                        tableMapEvent, 
                        this, 
                        pair.getBefore(), 
                        pair.getAfter(),
                        event.getBinlogFilename(),
                        event.getHeader().getPosition(),
                        event.getHeader().getTimestamp(),
                        i,
                        pairList.size());
                rowEventList.add(rowEvent);
            }
            return rowEventList;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return ((UpdateRowsEventV2) event).getRows().size();
        }

    },
    DELETE_ROWS_EVENT_HANDLER(DELETE_ROWS_EVENT, true) {

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            receiver.handleRowEvent((AbstractRowEvent) event);            
        }
      
        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent _event,
                TableMapEvent tableMapEvent) {
            
            DeleteRowsEvent event = (DeleteRowsEvent) _event;
            final List<Row> rowList = event.getRows();
            final List<MysqlBinlogTask> rowEventList = new ArrayList<>(rowList.size());
            
            for(int i=0; i<rowList.size(); i++){
                final Row oldRow = rowList.get(i);
                MysqlBinlogTask rowEvent = MysqlBinlogTask.getInstance(
                        processor, 
                        tableMapEvent, 
                        this, 
                        oldRow, 
                        null,
                        event.getBinlogFilename(),
                        event.getHeader().getPosition(),
                        event.getHeader().getTimestamp(),
                        i,
                        rowList.size());
                rowEventList.add(rowEvent);
            }
            return rowEventList;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return ((DeleteRowsEvent) event).getRows().size();
        }
    },
    DELETE_ROWS_EVENT_V2_HANDLER(DELETE_ROWS_EVENT_V2, true) {

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            receiver.handleRowEvent((AbstractRowEvent) event);            
        }

        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent _event,
                TableMapEvent tableMapEvent) {
            
            DeleteRowsEventV2 event = (DeleteRowsEventV2) _event;
            final List<Row> rowList = event.getRows();
            final List<MysqlBinlogTask> rowEventList = new ArrayList<>(rowList.size());
            
            for(int i=0; i<rowList.size(); i++){
                final Row oldRow = rowList.get(i);
                MysqlBinlogTask rowEvent = MysqlBinlogTask.getInstance(
                        processor, 
                        tableMapEvent, 
                        this, 
                        oldRow, 
                        null,
                        event.getBinlogFilename(),
                        event.getHeader().getPosition(),
                        event.getHeader().getTimestamp(),
                        i,
                        rowList.size());
                rowEventList.add(rowEvent);
            }
            return rowEventList;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return ((DeleteRowsEventV2) event).getRows().size();
        }
    },
    ROTATE_EVENT_HANDLER (ROTATE_EVENT, false){

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            receiver.handleRotateEvent((RotateEvent) event);
        }

        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent event,
                TableMapEvent tableMapEvent) {
            return null;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return 0;
        }
    },
    NOT_IMPLEMENTED_EVENT_HANDLER (-1, false){

        @Override
        public void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event) {
            // SKIP!!
        }

        @Override
        public List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
                MysqlBinlogProcessor processor,
                AbstractRowEvent event,
                TableMapEvent tableMapEvent) {
            return null;
        }

        @Override
        public int getRowCount(AbstractRowEvent event) {
            return 0;
        }
    }
    ;
    
    //=============================
    //
    //  enum static
    //
    //=============================
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogHandler.class);
    private static final MysqlBinlogHandler[] typeList = new MysqlBinlogHandler[64];
    
    static{
        
        for(int i=0; i<typeList.length; i++){
            typeList[i] = NOT_IMPLEMENTED_EVENT_HANDLER;
        }
        
        for(final MysqlBinlogHandler type : MysqlBinlogHandler.values()){
            if(NOT_IMPLEMENTED_EVENT_HANDLER == type){
                continue;
            }
            typeList[type.eventCode] = type;
        }
        
    }
    
    public static MysqlBinlogHandler valueOf(final int eventCode){
        return typeList[eventCode];
    }
    
    public static MysqlBinlogHandler valueOf(final BinlogEventV4Header eventHeader){
        return valueOf(eventHeader.getEventType());
    }
    
    public static MysqlBinlogHandler valueOf(final BinlogEventV4 event){
        return valueOf(event.getHeader());
    }
    
    //=============================
    //
    //  enum non-static
    //
    //=============================
    
    private final int eventCode;
    private final boolean isRowsEventHandler;
    private MysqlBinlogHandler(int eventCode, boolean isRowsEventHandler){
        this.eventCode = eventCode;
        this.isRowsEventHandler = isRowsEventHandler;
    }
    
    public boolean isRowsEventHandler(){
        return isRowsEventHandler;
    }
    
    public abstract void mysqlBinlogReceiverOnEventsHandler(MysqlBinlogReceiver receiver, BinlogEventV4 event);
    public abstract List<MysqlBinlogTask> generateMysqlBinlogRowEventTask(
            MysqlBinlogProcessor processor, 
            AbstractRowEvent event,
            TableMapEvent tableMapEvent);
    public abstract int getRowCount(AbstractRowEvent event);
        
}
