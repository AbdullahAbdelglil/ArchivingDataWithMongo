package com.esmartsoft.eg.hawkeyes.history;

import com.mongodb.client.*;

import org.bson.Document;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArchivingHistoricalDataWithMongoDB {
    private final String sourceDbUri = "mongodb://172.16.25.209:27017";
    private final String targetDbUri = "mongodb://localhost:27017";
    private final String sourceDbName = "imageDatabase";
    private final String targetDbName = "imageDatabaseArchive";
    private final String sourceCollectionName = "imageCollection";
    private final String targetCollectionName = "imageCollection";

    private MongoClient sourceClient;
    private MongoClient targetClient;
    private MongoDatabase sourceDB;
    private MongoDatabase targetDB;
    private MongoCollection<Document> sourceCollection;
    private MongoCollection<Document> targetCollection;

    private static final Map<Integer, List<String>> quarters = new HashMap<>();

    private ArchivingHistoricalDataWithMongoDB() {
    }

    public static ArchivingHistoricalDataWithMongoDB getInstance() {
        identifyQuarters();
        return new ArchivingHistoricalDataWithMongoDB();
    }

    private boolean connectToServer() {
        try {
            sourceClient = MongoClients.create(sourceDbUri);
            targetClient = MongoClients.create(targetDbUri);

            sourceDB = sourceClient.getDatabase(sourceDbName);
            targetDB = targetClient.getDatabase(targetDbName);

            sourceCollection = sourceDB.getCollection(sourceCollectionName);
            targetCollection = targetDB.getCollection(targetCollectionName);

            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void copyQuarter(int year, int quarter) {

        boolean connected = connectToServer();

        if (connected) {
            if (quarter >= 1 && quarter <= 4) {
                String startOfQuarter = (year + quarters.get(quarter).get(0));
                String endOfQuarter = (year + quarters.get(quarter).get(1));

                FindIterable<Document> sourceDocs = sourceCollection.find(
                        Filters.and(
                                Filters.gte("creation_date",startOfQuarter),
                                Filters.lte("creation_date",endOfQuarter)
                        )
                );

                for (Document document : sourceDocs) {
                    String documentGuid = document.getString("guid");

                    if (notExistInTargetCollection(documentGuid)) {
                        targetCollection.insertOne(document);
                    }
                }

            } else if (quarter==0) {
                copyYear(year);
            }

            closeConnections();
        }
    }

    private static void identifyQuarters() {
        String startOfTheDay = "T01:00:00.000Z";
        String endOfTheDay = "T23:59:59.999Z";

        quarters.put(0, Arrays.asList("-01-01" + startOfTheDay, "-12-31" + endOfTheDay));
        quarters.put(1, Arrays.asList("-01-01" + startOfTheDay, "-03-31" + endOfTheDay));
        quarters.put(2, Arrays.asList("-04-01" + startOfTheDay, "-06-30" + endOfTheDay));
        quarters.put(3, Arrays.asList("-07-01" + startOfTheDay, "-09-30" + endOfTheDay));
        quarters.put(4, Arrays.asList("-10-01" + startOfTheDay, "-12-31" + endOfTheDay));
    }

    private void copyYear(int year) {
        copyQuarter(year, 1);
        copyQuarter(year, 2);
        copyQuarter(year, 3);
        copyQuarter(year, 4);
    }

    private boolean notExistInTargetCollection(String guid) {
        Document doc = targetCollection.find(Filters.eq("guid", guid)).first();
        return (doc == null);
    }

    private void closeConnections() {
        sourceClient.close();
        targetClient.close();
    }
}

