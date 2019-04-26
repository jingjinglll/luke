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

import com.dell.pravegasearch.common.metrics.MetricsTimer;
import com.dell.pravegasearch.common.synchronizer.PravegaStoreFactory.StoreType;
import com.dell.pravegasearch.common.synchronizer.structure.PravegaSharedObjectSet;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaObjectSynchronizer extends PravegaSynchronizer
        implements ObjectSetSynchronizer {

    private static final Logger log = LoggerFactory.getLogger(PravegaObjectSynchronizer.class);
    private PravegaSharedObjectSet pravegaStore;
    private String rootPath;

    public PravegaObjectSynchronizer() {
        this("psearch", "defaultObject", "rootPath");
    }

    public PravegaObjectSynchronizer(String scopeName, String streamName, String rootPath) {
        this(scopeName, streamName, rootPath, StoreType.OBJECT, false);
        this.rootPath = rootPath;
    }

    public PravegaObjectSynchronizer(String scopeName, String streamName, String rootPath, StoreType type, boolean sorted ) {
        super(scopeName, streamName, type, sorted);
        this.pravegaStore = getStore();
        this.rootPath = rootPath;
    }

    @Override
    public byte[] getValueAsBytes(String key) {
        String path = rootPath + "/" + key;
        try {
            pravegaStore.refresh();
            if (pravegaStore.containsKey(path)) {
                MetricsTimer startTime = metrics.startTime();
                byte[] value = pravegaStore.get(path);
                metrics.reportGetValue(startTime);
                return value;
            }
        } catch (Exception e) {
            log.error("Failed to get {} from State Synchronizer", key, e);
            throw e;
        }
        return null;
    }

    @Override
    public void setValueAsBytes(String key, byte[] value) {
        String path = rootPath + "/" + key;
        try {
            MetricsTimer startTime = metrics.startTime();
            pravegaStore.put(path, value);
            metrics.reportSetValue(startTime);
        } catch (Exception e) {
            log.error("Failed to set {} into State Synchronizer", key, e);
            throw e;
        }
    }

    @Override
    public void setValueAsString(String key, String value) {
        byte[] bytes = value.getBytes();
        setValueAsBytes(key, bytes);
    }

    @Override
    public String getValueAsString(String key) {
        byte[] bytes = getValueAsBytes(key);
        if (bytes == null) {
            return null;
        }
        return new String(bytes);
    }

    @Override
    public <T> void setValueAsObject(String key, T value) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(value);
            out.flush();
            out.close();
            byte[] data = bos.toByteArray();
            setValueAsBytes(key, data);
        } catch (IOException e) {
            log.error("Failed to set item {} into State Synchronizer", key, e);
        }
    }

    @Override
    public <T> T getValueAsObject(String key) {
        byte[] data = getValueAsBytes(key);
        if (data == null) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        try {
            ObjectInputStream in = new ObjectInputStream(bis);
            return (T) in.readObject();
        } catch (IOException e) {
            log.error("Synchronizer ValueAsObject", e);
        } catch (ClassNotFoundException e) {
            log.error("Synchronizer ClassNotFoundException", e);
        }
        return null;
    }

    @Override
    public List<String> listKeys() {
        List<String> results = new ArrayList<>();
        try {
            pravegaStore.refresh();
            pravegaStore.entrySet().forEach(e -> {
                String key = e.getKey();
                String[] parts = key.split("/");
                if (parts.length == 2 && parts[0].equals(this.rootPath)) {
                    results.add(parts[1]);
                }
            });
            return results;
        } catch (Exception e) {
            log.error("Synchronizer List Keys Exception", e);
            throw e;
        }
    }

    @Override
    public void remove(String key) {
        String path = rootPath + "/" + key;
        try {
            pravegaStore.remove(path);
        } catch (Exception e) {
            log.error("Failed to remove item {} from Synchronizer", key, e);
            throw e;
        }
    }
}
