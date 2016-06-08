package com.kakao.adt.mysql.binlog.receiver;

import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;

public interface MysqlBinlogReceiverEventListener {

    void onReceivedRowEvent(AbstractRowEvent event);
    void onRotate(RotateEvent event);
    void onTableMapEvent(TableMapEvent event);
    void onException(MysqlBinlogReceiver receiver, BinlogParser parser, Exception exception);
    void onStop();
}
