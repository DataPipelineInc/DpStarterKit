package com.datapipeline.mongodb;

import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.inc;

public enum MongoDBHelper {

    INSTANCE;

    private static MongoClient mongoClient;

    /**
     * 获取客户端连接
     * @return
     */
    public void getMongoClient(String host, Integer port){
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(new ServerAddress(host, port))))
                        .build());
    }

    /**
     * 获取数据库实例
     * @param dbName
     * @return
     */
    public MongoDatabase getDB(String dbName) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        return database;
    }

    /**
     * 获取集合
     * @param dbName
     * @param collName
     * @return
     */
    public MongoCollection<Document> getCollection(String dbName, String collName){
        MongoCollection<Document> coll = mongoClient.getDatabase(dbName).getCollection(collName);
        return coll;
    }

}
