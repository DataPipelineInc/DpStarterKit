package com.datapipeline.starter;

import com.datapipeline.base.connector.sink.pipe.message.MemoryBatchMessage;
import com.datapipeline.clients.connector.schema.base.DpSinkRecord;
import com.datapipeline.clients.connector.schema.base.PrimaryKey;
import com.datapipeline.mongodb.MongoDBHelper;
import com.datapipeline.sink.connector.starterkit.DpSinkPipe;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.bson.Document;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;

public class MongoDBSinkPipe extends DpSinkPipe {

    private Logger logger = LoggerFactory.getLogger(MongoDBSinkPipe.class);

    private static Map<String, String> map = new HashMap<>();

    @Override
    public void init(Map<String, String> config) {
        if(StringUtils.isEmpty(config.get("dbname"))){
            logger.info("MongoDB dbname not empty");
            throw new RuntimeException("MongoDB dbname not empty");
        }
        if(StringUtils.isEmpty(config.get("host"))){
            logger.info("MongoDB host not empty");
            throw new RuntimeException("MongoDB host not empty");
        }
        if(StringUtils.isEmpty(config.get("port"))){
            logger.info("MongoDB port not empty");
            throw new RuntimeException("MongoDB port not empty");
        }
        MongoDBHelper.INSTANCE.createMongoClient(config.get("dbname"), config.get("host"), Integer.valueOf(config.get("port")));
    }

    @Override
    public void onStopped() {
        logger.info("close mongo client");
        MongoDBHelper.INSTANCE.closeMongoClient();
    }

    @Override
    public void handleSchemaChange(ConnectSchema lastSchema, ConnectSchema currSchema, String dpSchemaName, PrimaryKey primaryKey, boolean shouldStageData) {

        if(null != lastSchema && null != currSchema){
            Object[] lastArray = lastSchema.fields().stream().map(Field::name).toArray();
            Object[] currArray = currSchema.fields().stream().map(Field::name).toArray();
            MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection(dpSchemaName);
            fieldSync(lastArray, currArray, collection);
        }
    }

    @Override
    public void handleDelete(MemoryBatchMessage msg, String dpSchemaName) {
        logger.info("dptask#" + getContext().getDpTaskId() + " Data deletion of " + dpSchemaName);
        logger.info("Primary keys of the deletion are " + msg.getDpSinkRecords().keySet().stream().map(pk -> "'" + pk.getCompositeValue() + "'").collect(Collectors.joining(", ")));
        MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection(dpSchemaName);
        List<WriteModel<Document>> batchData = new ArrayList<>(msg.getDpSinkRecords().size());
        Set<PrimaryKey> primaryKeys = msg.getDpSinkRecords().keySet();
        for (PrimaryKey primaryKey : primaryKeys) {
            ImmutablePair<String, Object> immutablePair = primaryKey.getPrimaryKeys().get(0);
            WriteModel<Document> iom = new DeleteOneModel(eq(immutablePair.getKey(), immutablePair.getValue()));
            batchData.add(iom);
        }
        BulkWriteResult bulkWriteResult = collection.bulkWrite(batchData);
        logger.info(bulkWriteResult.toString());
    }

    @Override
    public void handleInsert(MemoryBatchMessage msg, String dpSchemaName, boolean shouldStageData) {
        MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection(dpSchemaName);
        List<WriteModel<Document>> batchData = new ArrayList<>(msg.getDpSinkRecords().size());
        Collection<DpSinkRecord> dpSinkRecords = msg.getDpSinkRecords().values();
        for (DpSinkRecord dpSinkRecord : dpSinkRecords) {
            try {
                PrimaryKey primaryKey = dpSinkRecord.getPrimaryKeys();
                ImmutablePair<String, Object> immutablePair = primaryKey.getPrimaryKeys().get(0);
                if(null == map.get(immutablePair.getKey())){
                    String index = collection.createIndex(new Document(immutablePair.getKey(), 1));
                    map.put(immutablePair.getKey(), index);
                    logger.info(index);
                }
                JSONObject dataJson = dpSinkRecord.getDataJson();
                UpdateOptions updateOptions = new UpdateOptions();
                updateOptions.upsert(true);
                WriteModel<Document> iom = new UpdateOneModel(eq(immutablePair.getKey(), immutablePair.getValue()), new Document("$set", getDocument(dataJson)), updateOptions);
                batchData.add(iom);
            } catch (Exception e) {
                throw new RuntimeException("Data type conversion error");
            }
        }
        BulkWriteResult result = collection.bulkWrite(batchData);
        logger.info(result.toString());
    }

    @Override
    public void handleSnapshotStart(String dpSchemaName, PrimaryKey primaryKey, ConnectSchema sinkSchema) {
        logger.info("dptask#" + getContext().getDpTaskId() + " Snapshot start of " + dpSchemaName);
    }

    @Override
    public void handleSnapshotDone(String dpSchemaName, PrimaryKey primaryKey) {
        logger.info("dptask#" + getContext().getDpTaskId() + " Snapshot done of " + dpSchemaName);
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
     * Field synchronization
     * @param lastArray last Array
     * @param currArray curr Array
     * @param collection mongodb collection
     */
    private void fieldSync(Object[] lastArray, Object[] currArray, MongoCollection<Document> collection){
        Set<Object> tempSet = new HashSet<>();
        Set<Object> lastTempSet = new HashSet<>();
        Set<Object> currTempSet = new HashSet<>();
        for(Object lastObj : lastArray){
            for(Object currObj : currArray){
                if(lastObj.equals(currObj)){
                    tempSet.add(currObj);
                }
            }
        }
        for (Object lastObj : lastArray){
            lastTempSet.add(lastObj);
        }
        for (Object currObj : currArray){
            currTempSet.add(currObj);
        }
        lastTempSet.removeAll(tempSet);
        currTempSet.removeAll(tempSet);
        for(Object lastTemp : lastTempSet){
            collection.updateMany(exists(String.valueOf(lastTemp), true), new Document("$unset", new Document(String.valueOf(lastTemp),"")));
        }
        for (Object currTemp : currTempSet){
            collection.updateMany(exists(String.valueOf(currTemp), false), new Document("$set", new Document(String.valueOf(currTemp),null)));
        }
    }

}
