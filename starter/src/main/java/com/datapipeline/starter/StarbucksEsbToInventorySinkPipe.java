package com.datapipeline.starter;

import com.datapipeline.base.connector.sink.pipe.message.MemoryBatchMessage;
import com.datapipeline.clients.JdbcConnect.Config;
import com.datapipeline.clients.JdbcConnectFactory;
import com.datapipeline.clients.connector.schema.base.PrimaryKey;
import com.datapipeline.clients.mysql.MySQLConnect;
import com.datapipeline.sink.connector.starterkit.DpSinkPipe;
import java.sql.SQLException;
import java.util.Map;
import org.apache.kafka.connect.data.ConnectSchema;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class StarbucksEsbToInventorySinkPipe extends DpSinkPipe {

    private MySQLConnect mySQLConnect;

    private long eid;

    @Override
    public void init(Map<String, String> config) {
        // Initialize mysql connection pool.
        mySQLConnect =
                JdbcConnectFactory.getInstance(
                        MySQLConnect::new,
                        new Config("172.17.0.1", "3307", "inventory", "debezium", "dbz"),
                        null);

        // retrieve processed eid from database.
    }

    @Override
    public void onStopped() {
        if (mySQLConnect != null) {
            mySQLConnect.close();
        }
    }

    @Override
    public void handleSchemaChange(
            ConnectSchema lastSchema,
            ConnectSchema currSchema,
            String dpSchemaName,
            PrimaryKey primaryKey,
            boolean shouldStageData) {
        // do nothing.
    }

    @Override
    public void handleDelete(MemoryBatchMessage msg, String dpSchemaName) {
        // do nothing.
    }

    @Override
    public void handleInsert(MemoryBatchMessage msg, String dpSchemaName, boolean shouldStageData) {

        msg.getDpSinkRecords()
                .values()
                .forEach(
                        dpSinkRecord -> {
                            try {
                                JSONObject data = dpSinkRecord.getDataJson();
                                long dataEid = data.getLong("eid");
                                String content = data.getString("content");

                                // ignore processed eid.
                                if (dataEid < eid) {
                                    // ignore
                                    return;
                                }
                                // Process content.

                                // update database for content and eid
                                mySQLConnect.doRetriableRequest(
                                        mySQLConnect
                                                .requestBuilder()
                                                .autoCommit(false)
                                                .execute("insert ...")
                                                .execute("update ...")
                                                .build());
                                // update eid
                                this.eid = dataEid;

                            } catch (JSONException | SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void handleSnapshotStart(
            String dpSchemaName, PrimaryKey primaryKey, ConnectSchema sinkSchema) {
        // do nothing.
    }

    @Override
    public void handleSnapshotDone(String dpSchemaName, PrimaryKey primaryKey) {
        // do nothing.
    }
}
