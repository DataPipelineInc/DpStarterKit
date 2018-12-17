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
            Object[] lastStrArray = lastSchema.fields().stream().map(Field::name).toArray();
            Object[] currStrArray = currSchema.fields().stream().map(Field::name).toArray();
            MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection(dpSchemaName);
            if(currStrArray.length > lastStrArray.length){
                Set<Object> diffElem = getDifferentElements(lastStrArray, currStrArray);
                for (Object str : diffElem) {
                    collection.updateMany(exists(String.valueOf(str), false), new Document("$set", new Document(String.valueOf(str),null)));
                }
            }
            if(currStrArray.length < lastStrArray.length){
                Set<Object> diffElem = getDifferentElements(currStrArray, lastStrArray);
                for (Object str : diffElem) {
                    collection.updateMany(exists(String.valueOf(str), true), new Document("$unset", new Document(String.valueOf(str),"")));
                }
            }
            if(currStrArray.length == lastStrArray.length){
                Set<Object> diffElem = getDifferentElements(lastStrArray, currStrArray);
                for (Object str : diffElem) {
                    String[] strArray = String.valueOf(str).split(",");
                    collection.updateMany(exists(strArray[0], true), new Document("$rename", new Document(strArray[0], strArray[1])));
                }
            }
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
                JSONObject dataJson = dpSinkRecord.getDataJson();
                UpdateOptions updateOptions = new UpdateOptions();
                updateOptions.upsert(true);
                WriteModel<Document> iom = new UpdateOneModel(eq(immutablePair.getKey(), immutablePair.getValue()), new Document("$set", getDocument(dataJson)), updateOptions);
                batchData.add(iom);
            } catch (JSONException e) {
                throw new RuntimeException("Data type conversion error");
            }
        }
        BulkWriteResult bulkWriteResult = collection.bulkWrite(batchData);
        logger.info(bulkWriteResult.toString());
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
     * Get two different elements of an array
     * @param small Small array
     * @param large large array
     * @return
     */
    private Set<Object> getDifferentElements(Object[] small, Object[] large){
        Set<Object> same = new HashSet<>();
        Set<Object> temp = new HashSet<>();

        if(small.length == large.length){
            for (int i = 0; i < small.length; i++) {
                Object sm = small[i];
                Object lg = large[i];
                if(!sm.equals(lg)){
                    same.add(sm+","+lg);
                }
            }
        }else {
            for (int i = 0; i < small.length; i++) {
                temp.add(small[i]);
            }
            for (int j = 0; j < large.length; j++) {
                if (temp.add(large[j])) {
                    same.add(large[j]);
                }
            }
        }
        return same;
    }

}
