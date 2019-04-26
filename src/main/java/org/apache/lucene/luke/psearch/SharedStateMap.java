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

import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SharedStateMap<K, V> implements Revisioned, Serializable {
    private static final long serialVersionUID = 1L;
    private final String scopedStreamName;
    private final Map<K, V> impl;
    private final Revision currentRevision;

    public SharedStateMap(String scopedStreamName, Map<K, V> impl, Revision revision) {
        this.scopedStreamName = scopedStreamName;
        this.impl = impl;
        this.currentRevision = revision;
    }

    @Override
    public Revision getRevision() {
        return currentRevision;
    }

    @Override
    public String getScopedStreamName() {
        return scopedStreamName;
    }

    public int size() {
        return impl.size();
    }

    public Set<K> keySet() {
        return impl.keySet();
    }

    public boolean containsKey(K key) {
        return impl.containsKey(key);
    }

    public boolean containsValue(V value) {
        return impl.containsValue(value);
    }

    public Set<Entry<K, V>> entrySet() {
        return impl.entrySet();
    }

    public V get(K key) {
        return impl.get(key);
    }

    public Collection<V> values() {
        return impl.values();
    }

    public Map<K, V> clone() {
        if (impl instanceof ConcurrentSkipListMap) {
            return new ConcurrentSkipListMap(impl);
        }
        return new ConcurrentHashMap<K, V>(impl);
    }

    public Map<K, V> getState() {
        return impl;
    }
}
