package com.datapipeline.starter;

import com.datapipeline.base.connector.sink.pipe.message.MemoryBatchMessage;
import com.datapipeline.clients.connector.schema.base.DpSinkRecord;
import com.datapipeline.clients.connector.schema.base.PrimaryKey;
import com.datapipeline.mongodb.MongoDBHelper;
import com.datapipeline.sink.connector.starterkit.DpSinkPipe;

import java.util.*;
import java.util.stream.Collectors;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;

<<<<<<< HEAD:starter/src/main/java/com/datapipeline/starter/ConsoleOutputPipe.java
public class ConsoleOutputPipe extends DpSinkPipe {
  @Override
  public void init(Map<String, String> config) {
=======
import org.bson.Document;
import org.codehaus.jettison.json.JSONObject;

import static com.mongodb.client.model.Filters.*;

public class ConsoleOutputSinkPipe extends DpSinkPipe {

  @Override
  public void init(Map<String, String> config) {
    //System.out.println("dptask#" + getContext().getDpTaskId() + " Pipe init.");
>>>>>>> DpStarterKit-fs:starter/src/main/java/com/datapipeline/starter/ConsoleOutputSinkPipe.java
    config.forEach((k, v) -> System.out.println("Key: " + k + " / Value: " + v));
    String host = config.get("host");
    String port = config.get("port");
    MongoDBHelper.INSTANCE.getMongoClient(host, Integer.valueOf(port));
  }

  @Override
  public void onStopped() {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Pipe stopped.");
  }

  @Override
  public void handleSchemaChange(ConnectSchema lastSchema, ConnectSchema currSchema, String
      dpSchemaName, PrimaryKey primaryKey, boolean shouldStageData) {
    /*System.out.println("dptask#" + getContext().getDpTaskId() + " Schema change of " +
        dpSchemaName);*/
    System.out.println("New schema has fields as " + currSchema.fields().stream().map
        (Field::name).collect(Collectors.joining(", ")));

    List<Field> lastFilelds = lastSchema.fields();
    List<Field> currFilelds = currSchema.fields();

  }

  @Override
  public void handleDelete(MemoryBatchMessage msg, String dpSchemaName) {
    /*System.out.println("dptask#" + getContext().getDpTaskId() + " Data deletion of " +
        dpSchemaName);*/
    System.out.println("Primary keys of the deletion are " + msg.getDpSinkRecords().keySet()
        .stream().map(pk -> "'" + pk.getCompositeValue() + "'").collect(Collectors.joining(", ")));
    MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection("test", "c1");
  }

  @Override
  public void handleInsert(MemoryBatchMessage msg, String dpSchemaName, boolean shouldStageData) {
    /*System.out.println("dptask#" + getContext().getDpTaskId() + " Data insertion of " +
        dpSchemaName + ", should staging data ? " + shouldStageData);*/
    MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection("test", "c1");
    msg.getDpSinkRecords().values().forEach(dpSinkRecord -> {
      System.out.println("Insert " + dpSinkRecord.getDataJson());
      JSONObject json = dpSinkRecord.getDataJson();
      collection.insertOne(Document.parse(json.toString()));
    });

  }

  @Override
  public void handleSnapshotStart(String dpSchemaName, PrimaryKey primaryKey, ConnectSchema
      sinkSchema) {
    /*System.out.println("dptask#" + getContext().getDpTaskId() + " Snapshot start of " +
        dpSchemaName);*/
  }

  @Override
  public void handleSnapshotDone(String dpSchemaName, PrimaryKey primaryKey) {
    /*System.out.println("dptask#" + getContext().getDpTaskId() + " Snapshot done of " +
        dpSchemaName);*/
  }

}
