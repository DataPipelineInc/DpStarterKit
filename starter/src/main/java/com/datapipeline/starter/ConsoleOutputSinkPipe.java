package com.datapipeline.starter;

import com.datapipeline.base.connector.sink.pipe.message.MemoryBatchMessage;
import com.datapipeline.clients.connector.schema.base.DpRecordKey;
import com.datapipeline.clients.utils.JsonConvert;
import com.datapipeline.sink.connector.starterkit.DpStarterLoadPipe;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;

public class ConsoleOutputSinkPipe extends DpStarterLoadPipe {
    @Override
    public void init(JsonNode config) {
        System.out.println(JsonConvert.getJsonString(config));
    }

    @Override
    public void stop(long timeout) {
        System.out.println(
                "dptask#"
                        + getContext().getDpTaskId()
                        + " Pipe stopped with timeout "
                        + String.valueOf(timeout));
    }

    @Override
    public void handleSchemaChange(
            ConnectSchema lastSchema,
            ConnectSchema currSchema,
            String dpSchemaName,
            DpRecordKey dpRecordKey,
            boolean shouldStageData)
            throws Exception {
        System.out.println(
                "dptask#" + getContext().getDpTaskId() + " Schema change of " + dpSchemaName);
        System.out.println(
                "New schema has fields as "
                        + currSchema.fields().stream()
                                .map(Field::name)
                                .collect(Collectors.joining(", ")));
    }

    @Override
    public void handleDelete(MemoryBatchMessage msg, String dpSchemaName) {
        System.out.println(
                "dptask#" + getContext().getDpTaskId() + " Data deletion of " + dpSchemaName);
        System.out.println(
                "Primary keys of the deletion are "
                        + msg.getDpSinkRecords().keySet().stream()
                                .map(pk -> "'" + pk.getCompositeValue() + "'")
                                .collect(Collectors.joining(", ")));
    }

    @Override
    public void handleInsert(MemoryBatchMessage msg, String dpSchemaName, boolean shouldStageData) {
        System.out.println(
                "dptask#"
                        + getContext().getDpTaskId()
                        + " Data insertion of "
                        + dpSchemaName
                        + ", should staging data ? "
                        + shouldStageData);
        msg.getDpSinkRecords()
                .values()
                .forEach(
                        dpSinkRecord -> System.out.println("Insert " + dpSinkRecord.getDataJson()));
    }

    @Override
    public void handleTruncateData(
            String dpSchemaName, DpRecordKey dpRecordKey, ConnectSchema sinkSchema)
            throws Exception {
        System.out.println(
                "dptask#" + getContext().getDpTaskId() + " truncate data for " + dpSchemaName);
    }

    @Override
    public void handleSnapshotStart(
            String dpSchemaName,
            DpRecordKey dpRecordKey,
            ConnectSchema sinkSchema,
            boolean shouldStageData)
            throws Exception {
        System.out.println(
                "dptask#" + getContext().getDpTaskId() + " Snapshot start of " + dpSchemaName);
    }

    @Override
    public void handleSnapshotDone(
            MemoryBatchMessage msg,
            String dpSchemaName,
            DpRecordKey dpRecordKey,
            boolean shouldStageData)
            throws Exception {
        System.out.println(
                "dptask#" + getContext().getDpTaskId() + " Snapshot done of " + dpSchemaName);
    }
}
