package com.datapipeline.starter;

import com.datapipeline.base.connector.sink.pipe.message.MemoryBatchMessage;
import com.datapipeline.clients.connector.schema.base.DpSinkRecord;
import com.datapipeline.clients.connector.schema.base.PrimaryKey;
import com.datapipeline.clients.record.DpRecordMessageType;
import com.datapipeline.mongodb.MongoDBHelper;
import com.datapipeline.sink.connector.starterkit.DpSinkPipe;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.bson.Document;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;

public class ConsoleOutputSinkPipe extends DpSinkPipe {


  @Override
  public void init(Map<String, String> config) {
    config.forEach((k, v) -> System.out.println("Key: " + k + " / Value: " + v));
  }

  @Override
  public void onStopped() {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Pipe stopped.");
  }

  @Override
  public void handleSchemaChange(ConnectSchema lastSchema, ConnectSchema currSchema, String dpSchemaName, PrimaryKey primaryKey, boolean shouldStageData) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Schema change of " + dpSchemaName);
    System.out.println("New schema has fields as " + currSchema.fields().stream().map(Field::name).collect(Collectors.joining(", ")));
  }

  @Override
  public void handleDelete(MemoryBatchMessage msg, String dpSchemaName) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Data deletion of " + dpSchemaName);
    System.out.println("Primary keys of the deletion are " + msg.getDpSinkRecords().keySet().stream().map(pk -> "'" + pk.getCompositeValue() + "'").collect(Collectors.joining(", ")));
  }

  @Override
  public void handleInsert(MemoryBatchMessage msg, String dpSchemaName, boolean shouldStageData) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Data insertion of " + dpSchemaName + ", should staging data ? " + shouldStageData);
    msg.getDpSinkRecords().values().forEach(dpSinkRecord -> System.out.println("Insert " + dpSinkRecord.getDataJson()));

  }

  @Override
  public void handleSnapshotStart(String dpSchemaName, PrimaryKey primaryKey, ConnectSchema sinkSchema) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Snapshot start of " + dpSchemaName);
  }

  @Override
  public void handleSnapshotDone(String dpSchemaName, PrimaryKey primaryKey) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Snapshot done of " + dpSchemaName);
  }

}
