package com.datapipeline;

import com.datapipeline.mongodb.MongoDBHelper;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class mongdbTest {
    public static void main(String[] args) {
        MongoDBHelper.INSTANCE.createMongoClient("test","47.95.247.67", 5088);
        MongoCollection<Document> collection = MongoDBHelper.INSTANCE.getCollection("user_info");
        String index = collection.createIndex(new Document("id", 1));
        System.out.println(index);
    }
}
