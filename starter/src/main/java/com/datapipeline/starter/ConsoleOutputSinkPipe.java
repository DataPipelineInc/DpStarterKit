package com.datapipeline.starter;

import com.datapipeline.base.connector.sink.pipe.message.MemoryBatchMessage;
import com.datapipeline.clients.connector.schema.base.PrimaryKey;
import com.datapipeline.mongodb.MongoDBHelper;
import com.datapipeline.sink.connector.starterkit.DpSinkPipe;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.mongodb.client.MongoCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.bson.Document;
import org.codehaus.jettison.json.JSONObject;

public class ConsoleOutputSinkPipe extends DpSinkPipe {

  private String dbname = null;

  @Override
  public void init(Map<String, String> config) {
    config.forEach((k, v) -> System.out.println("Key: " + k + " / Value: " + v));
    // 连接MongoDB数据库
    dbname = config.get("dbname");
    String host = config.get("host");
    Integer port = Integer.valueOf(config.get("port"));
    MongoDBHelper.INSTANCE.getMongoClient(host, port);
  }

  @Override
  public void onStopped() {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Pipe stopped.");
  }

  @Override
  public void handleSchemaChange(ConnectSchema lastSchema, ConnectSchema currSchema, String
          dpSchemaName, PrimaryKey primaryKey, boolean shouldStageData) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Schema change of " +
            dpSchemaName);
    System.out.println("New schema has fields as " + currSchema.fields().stream().map
            (Field::name).collect(Collectors.joining(", ")));
    String str = currSchema.fields().stream().map(Field::name).collect(Collectors.joining(", "));
  }

  @Override
  public void handleDelete(MemoryBatchMessage msg, String dpSchemaName) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Data deletion of " +
            dpSchemaName);
    System.out.println("Primary keys of the deletion are " + msg.getDpSinkRecords().keySet()
            .stream().map(pk -> "'" + pk.getCompositeValue() + "'").collect(Collectors.joining(", ")));
  }

  @Override
  public void handleInsert(MemoryBatchMessage msg, String dpSchemaName, boolean shouldStageData) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Data insertion of " +
            dpSchemaName + ", should staging data ? " + shouldStageData);
    msg.getDpSinkRecords().values().forEach(dpSinkRecord -> System.out.println("Insert " +
            dpSinkRecord.getDataJson()));

    msg.getDpSinkRecords().values().forEach(dpSinkRecord -> {
      PrimaryKey primaryKey = dpSinkRecord.getPrimaryKeys();
      JSONObject json = dpSinkRecord.getDataJson();
      Document doc = new Document();
      ConnectSchema schema = dpSinkRecord.getConnectSchema();
      schema.fields().forEach( field -> {
        try {
          appendDoc(field, json, doc);
        } catch (Exception e) {
          throw new RuntimeException("Data type conversion error!");
        }
      });

      // 数据更新或插入MongoDB
      MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getDB(dbname).getCollection(dpSchemaName);
      List<String> ks = primaryKey.getPrimaryKeyNames();
      String pkName = ks.get(0);
      Document idDoc = new Document(pkName,doc.get(pkName));
      Document myDoc = collection.find(idDoc).first();
      if(null != myDoc){
        collection.deleteOne(idDoc);
        collection.insertOne(doc);
      }else{
        collection.insertOne(doc);
      }
    });
  }

  @Override
  public void handleSnapshotStart(String dpSchemaName, PrimaryKey primaryKey, ConnectSchema
          sinkSchema) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Snapshot start of " +
            dpSchemaName);
  }

  @Override
  public void handleSnapshotDone(String dpSchemaName, PrimaryKey primaryKey) {
    System.out.println("dptask#" + getContext().getDpTaskId() + " Snapshot done of " +
            dpSchemaName);
  }

  private void appendDoc(Field field, JSONObject json, Document doc) throws Exception {
    Schema scm = field.schema();
    String type = scm.type().getName();
    String name = scm.name();
    if(StringUtils.equals("int8", type)){
      Short obj = (Short) json.get(field.name());
      doc.append(field.name(),obj);
    }
    if(StringUtils.equals("int16", type)){
      Integer obj = (Integer) json.get(field.name());
      doc.append(field.name(),obj);
    }
    if(StringUtils.equals("int32", type)){
      Integer obj = (Integer) json.get(field.name());
      doc.append(field.name(),obj);
    }
    if(StringUtils.equals("int64", type)){
      if(StringUtils.equals(name, "io.debezium.time.Timestamp")){
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date obj = format.parse(json.get(field.name()).toString());
        doc.append(field.name(),obj);
      }else{
        Long obj = (Long) json.get(field.name());
        doc.append(field.name(),obj);
      }
    }
    if(StringUtils.equals("string", type)){
      if(StringUtils.equals(name, "io.debezium.time.ZonedTimestamp")){
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date obj = format.parse(json.get(field.name()).toString());
        doc.append(field.name(), obj);
      }else {
        String obj = (String) json.get(field.name());
        doc.append(field.name(), obj);
      }
    }
    if(StringUtils.equals("float32", type)){
      Float obj = (Float) json.get(field.name());
      doc.append(field.name(),obj);
    }
    if(StringUtils.equals("float64", type)){
      Double obj = (Double) json.get(field.name());
      doc.append(field.name(),obj);
    }
    if(StringUtils.equals("boolean", type)){
      Boolean obj = (Boolean) json.get(field.name());
      doc.append(field.name(),obj);
    }
    if(StringUtils.equals("bytes", type)){
      if(StringUtils.equals(name, "org.apache.kafka.connect.data.Decimal")){
        BigDecimal bd = (BigDecimal) json.get(field.name());
        String obj = bd.toPlainString();
        doc.append(field.name(), obj);
      }else {
        Byte obj = (Byte) json.get(field.name());
        doc.append(field.name(), obj);
      }
    }

  }
}
