package org.apache.lucene.luke.psearch;


import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class PravegaDirectory extends BaseDirectory {

    private static final Logger log = LoggerFactory.getLogger(PravegaDirectory.class);
//    private final static String CACHE_BASE_PATH = "/tmp/psearch/rocksdb/";

    private LuceneFileStore client;
//    private RocksDBCache cache;
    private AtomicLong nextTempFileCounter = new AtomicLong();
    private String shardId;

    public PravegaDirectory(String shardId) {
        this(new SingleInstanceLockFactory(), shardId);
    }

    public PravegaDirectory(LockFactory lockFactory, String shardId) {
        super(lockFactory);
        this.shardId = shardId;
        this.client = LuceneFileStoreFactory.getInstance(shardId);
//        this.cache = RocksDBCacheFactory.getCache(shardId, CACHE_BASE_PATH + shardId, cacheCapacity);
    }

    public void clear() {
        isOpen = false;
        LuceneFileStoreFactory.clear(shardId);
    }

    @Override
    public String[] listAll() throws IOException {
        Set<String> names = client.listLuceneFiles();
        log.info("Directory has files {}", names);
        String[] namesArray = names.toArray(new String[names.size()]);
        Arrays.sort(namesArray);
        return namesArray;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        client.deleteEvent(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
//        if (cache.keyExists(name)) {
//            byte[] data = cache.get(name);
//            return data.length;
//        }
        return client.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext ioContext) throws IOException {
        return new PravegaIndexOutput(name);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext ioContext) throws IOException {
        while (true) {
            String name = IndexFileNames.segmentFileName(prefix, suffix + "_" + Long.toString(nextTempFileCounter.getAndIncrement(), Character.MAX_RADIX), "tmp");
            return new PravegaIndexOutput(name);
        }
    }

    @Override
    public void sync(Collection<String> collection) throws IOException {
        for (String name : collection) {
            byte[] content = client.read(name);

            if (client.isSegFile(name)) {

                client.write(name, content);
            } else {

                client.send(name, content);

            }
        }

    }

    @Override
    public void rename(String source, String dest) throws IOException {
        client.commit(source, dest);
    }

    @Override
    public void syncMetaData() throws IOException {

    }

    @Override
    public IndexInput openInput(String name, IOContext ioContext) throws IOException {
        return new PravegaInputStream(name, client);
    }

    @Override
    public void close() throws IOException {
        LuceneFileStoreFactory.close(shardId);
    }
}
