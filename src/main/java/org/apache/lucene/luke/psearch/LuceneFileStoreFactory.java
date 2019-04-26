package org.apache.lucene.luke.psearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LuceneFileStoreFactory {
    private static Map<String, LuceneFileStore> fileStoreMap = new HashMap<>();

    public static synchronized LuceneFileStore getInstance(String shardId) {
        return fileStoreMap.computeIfAbsent(shardId, id -> new LuceneFileStore(id));
    }

    public static synchronized void close(String shardId) throws IOException {
        LuceneFileStore store = fileStoreMap.remove(shardId);
        if (store != null) {
            store.close();
        }
    }

    public static synchronized void clear(String shardId) {
        LuceneFileStore store = fileStoreMap.remove(shardId);
        if (store != null) {
            store.clear();
        }
    }
}