package com.datapipeline.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import org.bson.Document;
import java.util.Arrays;

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
                        /*.applyToConnectionPoolSettings(builder -> {

                        })*/
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

}
