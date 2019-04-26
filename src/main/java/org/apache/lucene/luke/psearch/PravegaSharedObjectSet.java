/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.luke.psearch;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaSharedObjectSet extends PravegaSharedStore<SharedStateMap<String, byte[]>> {

    private static final Logger log = LoggerFactory.getLogger(PravegaSharedObjectSet.class);
    private static final long MAX_BYTES = 32 * 1024 * 1024;

    private static class CreateState implements InitialUpdate<SharedStateMap<String, byte[]>>, Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<String, byte[]> impl;

        public CreateState(Map<String, byte[]> impl) {
            this.impl = impl;
        }

        @Override
        public SharedStateMap<String, byte[]> create(String scopedStreamName, Revision revision) {
            return new SharedStateMap(scopedStreamName, impl, revision);
        }
    }

    private static abstract class StateUpdate implements Update<SharedStateMap<String, byte[]>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public SharedStateMap<String, byte[]> applyTo(SharedStateMap<String, byte[]> oldState, Revision newRevision) {
            Map<String, byte[]> newState;
            if (oldState.getState() instanceof ConcurrentSkipListMap) {
                newState = new ConcurrentSkipListMap(oldState.getState());
            } else {
                newState = new ConcurrentHashMap(oldState.getState());
            }
            process(newState);
            return new SharedStateMap(oldState.getScopedStreamName(), newState, newRevision);
        }

        public abstract void process(Map<String, byte[]> updatableList);
    }

    private static class Clear extends StateUpdate {
        private static final long serialVersionUID = 1L;

        public Clear() {
        }

        @Override
        public void process(Map<String, byte[]> impl) {
            impl.clear();
        }
    }

    private static class Put extends StateUpdate {
        private static final long serialVersionUID = 1L;
        private final String key;
        private final byte[] value;

        public Put(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void process(Map<String, byte[]> impl) {
            impl.put(key, value);
        }
    }

    private static class PutAll extends StateUpdate {
        private static final long serialVersionUID = 1L;
        private final Map<String, byte[]> map;

        public PutAll(Map<String, byte[]> map) {
            this.map = map;
        }

        @Override
        public void process(Map<String, byte[]> impl) {
            impl.putAll(map);
        }
    }

    private static class Remove extends StateUpdate {
        private static final long serialVersionUID = 1L;
        private final String key;

        public Remove(String key) {
            this.key = key;
        }

        @Override
        public void process(Map<String, byte[]> impl) {
            impl.remove(key);
        }
    }

    public PravegaSharedObjectSet(ClientFactory clientFactory, StreamManager streamManager, String scope, String name) {
        this(clientFactory, streamManager, scope, name, false);
    }

    public PravegaSharedObjectSet(ClientFactory clientFactory, StreamManager streamManager,
                                  String scopeName, String streamName, boolean sorted) {
        streamManager.createScope(scopeName);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                                              .build();

        streamManager.createStream(scopeName, streamName, streamConfig);
        stateSynchronizer = clientFactory.createStateSynchronizer(streamName,
                new JavaSerializer<StateUpdate>(),
                new JavaSerializer<CreateState>(),
                SynchronizerConfig.builder().build());
        Map<String, byte[]> initMap = sorted ? new ConcurrentSkipListMap() : new ConcurrentHashMap();
        stateSynchronizer.initialize(new CreateState(initMap));
    }


    private void compact() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(stateSynchronizer.getState());
            out.flush();
            byte[] data = bos.toByteArray();
            if (data.length > MAX_BYTES) {
                log.warn("Failed to compact object synchronizer due to state too large.");
                return;
            } else {
                stateSynchronizer.compact(state -> new CreateState(state.getState()));
            }
        } catch (IOException e) {
            log.error("Failed to compact object in synchronizer.", e);
        }
    }

    @Override
    public void clear() {
        stateSynchronizer.updateState((state, updates) -> {
            if (state.size() > 0) {
                updates.add(new Clear());
            }
        });
        compact();
    }

    public Map<String, byte[]> clone() {
        return stateSynchronizer.getState().clone();
    }

    public Set<String> keys() {
        return stateSynchronizer.getState().keySet();
    }

    public boolean containsKey(String key) {
        return stateSynchronizer.getState().containsKey(key);
    }

    public Set<Map.Entry<String, byte[]>> entrySet() {
        return stateSynchronizer.getState().entrySet();
    }

    public byte[] get(String key) {
        return stateSynchronizer.getState().get(key);
    }

    public void put(String key, byte[] value) {
        stateSynchronizer.updateState((state, updates) -> {
            updates.add(new Put(key, value));
        });
    }

    public void putAll(Map<String, byte[]> map) {
        stateSynchronizer.updateState((state, updates) -> {
            updates.add(new PutAll(map));
        });
    }

    public byte[] putIfAbsent(String key, byte[] value) {
        final AtomicReference<byte[]> ret = new AtomicReference<byte[]>();

        refresh();  //this is a conditional modifying operation, need to update local state with current shared state before checking the condition
        stateSynchronizer.updateState((state, updates) -> {
            if (state.containsKey(key) && state.get(key) != null) {
                ret.set(state.get(key));
            } else {
                ret.set(null);
                updates.add(new Put(key, value));
            }
        });
        return ret.get();
    }

    public byte[] remove(String key) {
        final AtomicReference<byte[]> oldValue = new AtomicReference<byte[]>(null);
        stateSynchronizer.updateState((state, updates) -> {
            if (state.getState().containsKey(key)) {
                oldValue.set(state.get(key));
                updates.add(new Remove(key));
            } else {
                oldValue.set(null);
            }
        });

        return oldValue.get();
    }

    public int size() {
        return stateSynchronizer.getState().size();
    }
}
