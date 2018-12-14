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
      String lastStr = lastSchema.fields().stream().map(Field::name).collect(Collectors.joining(","));
      String currStr = currSchema.fields().stream().map(Field::name).collect(Collectors.joining(","));
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

    // 得到mongodb集合
    MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection(dpSchemaName);
    // 定义批量提交batchData
    List<WriteModel<Document>> batchData = new ArrayList<>(msg.getDpSinkRecords().size());
    // 获取数据类型（I:插入，U:更新）
    String type = msg.getBatchMeta().getMessageType().getType();
    // 循环放入批量提交集合requests
    Collection<DpSinkRecord> dpSinkRecords = msg.getDpSinkRecords().values();
    for (DpSinkRecord dpSinkRecord : dpSinkRecords) {
      try {
        PrimaryKey primaryKey = dpSinkRecord.getPrimaryKeys();
        ImmutablePair<String, Object> immutablePair = primaryKey.getPrimaryKeys().get(0);
        JSONObject dataJson = dpSinkRecord.getDataJson();
        // 数据去重
        if (StringUtils.equals("I", type)) {
          Document document = collection.find(eq(immutablePair.getKey(), immutablePair.getValue())).first();
          if (null != document && document.size() > 0) {
            continue;
          }
        }
        // 获取批量写入对象
        WriteModel<Document> iom = getBatchData(type, immutablePair, dataJson);
        batchData.add(iom);
      } catch (JSONException e) {
        throw new RuntimeException("Data type conversion error");
      }
    }
    // 执行批量提交
    BulkWriteResult bulkWriteResult = collection.bulkWrite(batchData);
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
   * @param type
   * @throws JSONException
   */
  private WriteModel<Document> getBatchData(String type, ImmutablePair<String, Object> immutablePair, JSONObject dataJson) throws JSONException {
    WriteModel<Document> iom = null;
    if(StringUtils.equals("U", type)){
      iom = new UpdateOneModel(eq(immutablePair.getKey(), immutablePair.getValue()), new Document("$set", getDocument(dataJson)));
    }
    if(StringUtils.equals("I", type)){
      iom = new InsertOneModel<>(getDocument(dataJson));
    }
    return iom;
  }
}
