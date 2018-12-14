package com.datapipeline.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.Arrays;

public enum MongoDBHelper {

    INSTANCE;

    private MongoClient mongoClient;

    private String dataBaseName;

    /**
     * Create Client Connection
     * @param host ip address
     * @param port ip port
     */
    public void createMongoClient(String dataBaseName, String host, Integer port){
        try {
            //MongoCredential credential = MongoCredential.createCredential(user, source, password);
            mongoClient = MongoClients.create(
                    MongoClientSettings.builder().applyToConnectionPoolSettings(builder -> {
                        builder.minSize(100);
                        builder.maxSize(1000);
                    }).applyToClusterSettings(builder -> {
                        builder.hosts(Arrays.asList(new ServerAddress(host, port)));
                    })
                            //.credential(credential)
                    .build());
            this.dataBaseName = dataBaseName;
        }catch (Exception e){
            throw new RuntimeException("创建mongoClient失败");
        }
    }

    /**
     * Get Client Connection
     * @return
     */
    public MongoClient getMongoClient(){
        return mongoClient;
    }

    /**
     * get collection
     * @param table table
     * @return
     */
    public MongoCollection<Document> getCollection(String table){
        return mongoClient.getDatabase(dataBaseName).getCollection(table);
    }

    /**
     * close Client
     */
    public void closeMongoClient() {
        if (null != mongoClient) {
            mongoClient.close();
        }
    }

}
