package org.apache.lucene.luke.psearch;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.netty.util.internal.ConcurrentSet;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

//FIXME: Not thread-safe because we believe lucene operations are all sequential.
public class LuceneFileStore {
    private static final Logger log = LoggerFactory.getLogger(LuceneFileStore.class);

    private String defaultScope;
    private StreamManager streamManager;
    private ClientConfig clientConfig;
    private StreamConfiguration streamConfig;
    private String shardId;
    /**
     We only keep latest commit point, which is the default IndexDeletionPolicy in Lucene
     */

    private CommitPoint commitPoint;
    private Map<String, byte[]> pendingCommitPoint = new ConcurrentHashMap<>();
    private Map<String, EventData> eventsMap = new ConcurrentHashMap<>();
    private Set<String> deletionPending = Collections.synchronizedSet(new HashSet<>());
    private ObjectSetSynchronizer synchronizer;
    private String rootPath;
    private final ReentrantReadWriteLock isClose = new ReentrantReadWriteLock();
    private final Lock notClose = isClose.readLock();
    private final Lock close = isClose.writeLock();
    private final ThreadPoolService threadPoolService =
            ResourceManager.ThreadPool.getThreadPoolService(ThreadPoolType.SHARD_STREAM_SEAL_AND_DELETE);
    private Set<String> streamSet = new ConcurrentSet<>();
    private Cache<String, ByteStreamWriter> writerCache;
    private Cache<String, ByteStreamReader> readerCache;
    private ByteStreamClient byteStreamClient;

    LuceneFileStore(String shardId) {
        this.shardId = shardId;
        init(shardId);
    }

    private void init(String shardId) {
        rootPath = "dataCommitPoint_    "+ shardId;
        String SCOPE = "dataCommitPointScope";
        String STREAM = "dataCommitPointStream";
        synchronizer = new PravegaObjectSynchronizer(
                SCOPE,
                STREAM,
                rootPath);
        defaultScope = "defaultDataDirectoryScope";
        ClientFactory clientFactory = PravegaConfigHelper.getClientFactory(defaultScope);

        byteStreamClient = clientFactory.createByteStreamClient();
        clientConfig = PravegaConfigHelper.getClientConfig();
        streamManager = StreamManager.create(clientConfig);
        streamManager.createScope(defaultScope);
        streamConfig = StreamConfiguration.builder()
                                          .scalingPolicy(ScalingPolicy.fixed(1))
                                          .build();

        CommitPoint commitPointTmp = synchronizer.getValueAsObject(rootPath);
        if (commitPointTmp != null) {
            commitPoint = commitPointTmp;
            eventsMap = commitPointTmp.getEventDataMap();
        }

    }

    public void write(String key, byte[] value) throws IOException {
        notClose.lock();
        try {

            log.info("Write lucene file {} to pravega", key);
            String segmentName = getStreamName(key);
            streamSet.add(segmentName);
            ByteStreamWriter writer = writerCache.getIfPresent(segmentName);
            if (writer == null) {
                streamManager.createStream(defaultScope, segmentName, streamConfig);
                writer = byteStreamClient.createByteStreamWriter(segmentName);
                writerCache.put(segmentName, writer);
            }
            ByteStreamReader reader = readerCache.getIfPresent(segmentName);
            if (reader == null) {
                reader = byteStreamClient.createByteStreamReader(segmentName);
                readerCache.put(segmentName, reader);
            }
            long tail = reader.fetchTailOffset();
            writer.write(value);
            eventsMap.put(key, new EventData(key, tail, value.length));
            writer.flush();


        } finally {
            notClose.unlock();
        }
    }

    public byte[] read(String key) throws IOException {
        notClose.lock();
        try {

            log.info("Receiving Lucene file {} from Pravega", key);
            streamSet.add(key);
            String segmentName = getStreamName(key);
            ByteStreamReader reader = readerCache.getIfPresent(segmentName);
            if (reader == null) {
                streamManager.createStream(defaultScope, segmentName, streamConfig);
                reader = byteStreamClient.createByteStreamReader(segmentName);
                readerCache.put(segmentName, reader);
            }
            EventData eventData = eventsMap.get(key);
            long offset = eventData.getOffset();
            int length = eventData.getLength();
            reader.seekToOffset(offset);
            byte[] res = new byte[length];
            reader.read(res);

            return res;
        } finally {
            notClose.unlock();
        }
    }

    public int fileLength(String key) {
        return eventsMap.getOrDefault(key, new EventData(key, 0, 0)).getLength();
    }

    public void send(String name, byte[] content) {
        log.info("Store the pending commit point {}", name);

        pendingCommitPoint.put(name, content);

    }

    public byte[] receiveAsBytes(String name) {
        log.info("Receiving Commit Point {} from synchronizer", name);

        if (pendingCommitPoint.containsKey(name)) {
            return pendingCommitPoint.get(name);
        }
        byte[] content = commitPoint.getCommitPoint();

        return content;
    }

    public Set<String> listLuceneFiles() {
        Set<String> files = new HashSet<>();
        files.addAll(eventsMap.keySet());
        files.addAll(pendingCommitPoint.keySet());
        if (commitPoint != null) {
            files.add(commitPoint.getName());
        }
        return files;
    }

    public void commit(String source, String dest) throws IOException {
        byte[] content = pendingCommitPoint.get(source);
        if (content == null) {
            throw new IOException("Pending CommitPoint not found");
        }

        CommitPoint commitPoint = new CommitPoint(eventsMap, dest, content);

        //Atomic Operation
        synchronizer.setValueAsObject(rootPath, commitPoint);


        pendingCommitPoint.remove(source);

        Runnable deletionRunner = () -> {
            log.info("About to delete streams {}", deletionPending);
            for (String streamName : new HashSet<>(deletionPending)) {
                try {
                    streamManager.sealStream(defaultScope, streamName);
                    streamManager.deleteStream(defaultScope, streamName);
                } finally {
                    deletionPending.remove(streamName);
                }
            }
        };

        threadPoolService.submit(deletionRunner);
    }

    private String getPrefix(String name) {
        String prefix  = name.indexOf('.') > 0 ? name.substring(0, name.indexOf('.')) : name;
        prefix = prefix.indexOf('_', 1) > 0 ? prefix.substring(0, prefix.indexOf('_', 1)) : prefix;
        return prefix;
    }

    public void deleteEvent(String name) throws IOException {
        log.info("About to delete event {}", name);
        if (isSegFile(name)) {
            EventData eventData = eventsMap.remove(name);
            String segmentName = getStreamName(name);
            String prefix = getPrefix(name);
            Set<String> set = eventsMap.keySet()
                                       .stream()
                                       .filter(s -> s.startsWith(prefix))
                                       .collect(Collectors.toSet());
            if (eventData != null && set.size() == 0) {
                deletionPending.add(segmentName);

                ByteStreamWriter writer = writerCache.getIfPresent(segmentName);
                if (writer != null) {
                    writer.close();
                    writerCache.invalidate(segmentName);
                }
                ByteStreamReader reader = readerCache.getIfPresent(segmentName);
                if (reader != null) {
                    reader.close();
                    readerCache.invalidate(segmentName);
                }
                streamSet.remove(segmentName);
            }
        } else {
            pendingCommitPoint.remove(name);
        }
    }

    public Set<String> listStreams() {
        return this.streamSet;
    }

    private String getStreamName(String fileName) {
        StringBuffer sb = new StringBuffer("_");
        String segmentName = fileName.split("\\.")[0];
        for (int i = 1; i < segmentName.length(); i++) {
            char c = segmentName.charAt(i);
            if (c != '_') {
                sb.append(c);
            } else {
                break;
            }
        }
        return this.shardId + sb.toString().replace('_', '-');
    }

    public boolean isSegFile(String name) {
        return name.charAt(0) == '_';
    }

    /* For Prefetcher to tail read */
    public <T> EventStreamReader<T> getEventReader(String streamName) {
        ClientFactory clientFactory = PravegaConfigHelper.getClientFactory(defaultScope);
        String readerGroup = UUID.randomUUID().toString();
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().build();
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(defaultScope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        EventStreamReader<T> reader = clientFactory.createReader("reader",
                readerGroup,
                (Serializer<T>) new JavaSerializer<>(),
                ReaderConfig.builder().build());
        return reader;
    }

    /**
     * Close streams and synchronizers relates to current shard.
     * @throws IOException IO exception thrown when closing the writers
     */
    public void close() throws IOException {
        close.lock();
        try {
            for (ByteStreamWriter writer : this.writerCache.asMap().values()) {
                writer.close();
            }
            for (ByteStreamReader reader : this.readerCache.asMap().values()) {
                reader.close();
            }
            if (synchronizer != null) {
                synchronizer.close();
            }
            //this.streamManager.deleteScope(defaultScope);
            streamManager.close();
            //threadPoolService.close();
        } finally {
            close.unlock();
        }
    }

    /**
     * Clear streams owned by current shard.
     * This method is only called when the shard is being deleted.
     */
    public void clear() {
        log.info("Deleting segments {}", streamSet);
        for (String streamName : streamSet) {
            streamManager.sealStream(defaultScope, streamName);
            streamManager.deleteStream(defaultScope, streamName);
        }
        synchronizer.remove(rootPath);
    }
}
