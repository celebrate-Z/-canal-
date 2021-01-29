package com.celebrate.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xpand.starter.canal.annotation.CanalEventListener;
import com.xpand.starter.canal.annotation.DeleteListenPoint;
import com.xpand.starter.canal.annotation.InsertListenPoint;

/**
 * 作者：zhouqinghe
 * 日期时间：2021/1/29 1:57 下午
 * 功能描述：
 */
@CanalEventListener
public class CanalListener {

    @InsertListenPoint
    public void ListenerInsert(CanalEntry.EventType canalEntry, CanalEntry.RowData rowData){
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            String columnName = column.getName();
            String columnValue = column.getValue();
            System.out.println("添加数据columnName = " + columnName+"columnValue = " + columnValue);
        }
    }

    @DeleteListenPoint
    public void Listenerdelet(CanalEntry.EventType canalEntry,CanalEntry.RowData rowData){
        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            String columnName = column.getName();
            String columnValue = column.getValue();
            System.out.println("之前columnName = " + columnName+"columnValue = " + columnValue);

        }
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            String columnName = column.getName();
            String columnValue = column.getValue();
            System.out.println("之后columnName = " + columnName+"columnValue = " + columnValue);
        }
    }
}
