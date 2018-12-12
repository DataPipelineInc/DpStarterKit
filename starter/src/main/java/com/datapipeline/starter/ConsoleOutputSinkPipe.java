package com.datapipeline.starter;

import com.datapipeline.base.connector.sink.pipe.message.MemoryBatchMessage;
import com.datapipeline.clients.connector.schema.base.DpSinkRecord;
import com.datapipeline.clients.connector.schema.base.PrimaryKey;
import com.datapipeline.mongodb.MongoDBHelper;
import com.datapipeline.sink.connector.starterkit.DpSinkPipe;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

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
    if(StringUtils.isEmpty(config.get("dbname"))){
      throw new RuntimeException("MongoDB dbname not empty");
    }
    if(StringUtils.isEmpty(config.get("host"))){
      throw new RuntimeException("MongoDB host not empty");
    }
    if(StringUtils.isEmpty(config.get("port"))){
      throw new RuntimeException("MongoDB port not empty");
    }
    MongoDBHelper.INSTANCE.createMongoClient(config.get("dbname"), config.get("host"), Integer.valueOf(config.get("port")));
  }

  @Override
  public void onStopped() {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Pipe stopped.");
    MongoDBHelper.INSTANCE.closeMongoClient();
  }

  @Override
  public void handleSchemaChange(ConnectSchema lastSchema, ConnectSchema currSchema, String dpSchemaName, PrimaryKey primaryKey, boolean shouldStageData) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Schema change of " + dpSchemaName);
    System.out.println("New schema has fields as " + currSchema.fields().stream().map(Field::name).collect(Collectors.joining(", ")));

    if(null != lastSchema && null != currSchema){
      String lastStr = lastSchema.fields().stream().map(Field::name).collect(Collectors.joining(", "));
      String currStr = currSchema.fields().stream().map(Field::name).collect(Collectors.joining(", "));
      String[] lastStrArray = lastStr.split(",");
      String[] currStrArray = currStr.split(",");
      if(lastStrArray.length != currStrArray.length){
        MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection(dpSchemaName);
        if(currStrArray.length > lastStrArray.length){
          Set<String> diffElem = getDifferentElements(lastStrArray, currStrArray);
          for (String str : diffElem) {
            collection.updateMany(exists(str, false), new Document("$set", new Document(str,null)));
          }
        }
        if(currStrArray.length < lastStrArray.length){
          Set<String> diffElem = getDifferentElements(currStrArray, lastStrArray);
          for (String str : diffElem) {
            collection.updateMany(exists(str, true), new Document("$unset", new Document(str,"")));
          }
        }
      }
    }
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
    MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection(dpSchemaName);
    List<WriteModel<Document>> requests = new ArrayList<>();
    msg.getDpSinkRecords().values().forEach(dpSinkRecord ->
      {
        try {
          getBatchData(dpSinkRecord, collection, requests);
        } catch (JSONException e) {
          throw new RuntimeException("Data type conversion error");
        }
      }
    );
    BulkWriteResult bulkWriteResult = collection.bulkWrite(requests);
    System.out.println(bulkWriteResult.toString());
  }

  @Override
  public void handleSnapshotStart(String dpSchemaName, PrimaryKey primaryKey, ConnectSchema sinkSchema) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Snapshot start of " + dpSchemaName);
  }

  @Override
  public void handleSnapshotDone(String dpSchemaName, PrimaryKey primaryKey) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Snapshot done of " + dpSchemaName);
  }

  /**
   * Document object converted to MongoDB based on JSON
   * @param jsonObj
   * @return
   * @throws JSONException
   */
  private Document getDocument(JSONObject jsonObj) throws JSONException {
    Document document = new Document();
    Iterator iterator = jsonObj.keys();
    while (iterator.hasNext()) {
      String key = iterator.next().toString();
      Object obj = jsonObj.get(key);
      if (obj instanceof BigDecimal) {
        obj = obj.toString();
      }
      document.put(key, obj);
    }
    return document;
  }

  /**
   * Get two different elements of an array
   * @param small Small array
   * @param large large array
   * @return
   */
  private Set<String> getDifferentElements(String[] small, String[] large){
    Set<String> same = new HashSet<>();
    Set<String> temp = new HashSet<>();
    for (int i = 0; i < small.length; i++) {
      temp.add(small[i]);
    }
    for (int j = 0; j < large.length; j++) {
      if(temp.add(large[j])) {
        same.add(large[j].trim());
      }
    }
    return same;
  }

  /**
   * get batch data
   * @param dpSinkRecord
   * @param collection
   * @param requests
   * @throws JSONException
   */
  private void getBatchData(DpSinkRecord dpSinkRecord, MongoCollection<Document> collection, List<WriteModel<Document>> requests) throws JSONException {
    JSONObject jsonObj = dpSinkRecord.getDataJson();
    PrimaryKey primaryKey = dpSinkRecord.getPrimaryKeys();
    List<ImmutablePair<String, Object>> primaryKeys = primaryKey.getPrimaryKeys();
    for (ImmutablePair<String, Object> immutablePair : primaryKeys) {
      Document documentOne = collection.find(eq(immutablePair.getKey(), immutablePair.getValue())).first();
      if(null != documentOne && documentOne.size() > 0){
        UpdateOneModel iom = new UpdateOneModel(eq(immutablePair.getKey(), immutablePair.getValue()), new Document("$set", getDocument(jsonObj)));
        requests.add(iom);
      }else{
        InsertOneModel<Document> iom = new InsertOneModel<>(getDocument(jsonObj));
        requests.add(iom);
      }
    }
  }
}
