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
package com.dell.pravegasearch.common.synchronizer;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import com.dell.pravegasearch.config.PravegaConfigHelper;
import com.dell.pravegasearch.PravegaSharedStore;
import com.dell.pravegasearch.PravegaStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaSynchronizer implements Synchronizer {
    private static final Logger log = LoggerFactory.getLogger(PravegaSynchronizer.class);

    private final PravegaSharedStore sharedStore;

    public PravegaSynchronizer(String scopeName, String streamName, PravegaStoreFactory.StoreType storeType, boolean sorted ) {
        Preconditions.checkArgument(!scopeName.contains("/"), "Root path cannot contain slash");
//        ClientConfig clientConfig = null;
//        try {
//            clientConfig = ClientConfig.builder()
//                                                    .controllerURI(new URI("tcp://localhost:9090"))
//                                                    .credentials(new DefaultCredentials("password", "admin"))
//                                                    .trustStore("pravegadummycerts/pravegacert.pem")
//                                                    .validateHostName(false)
//                                                    .build();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        ClientFactory clientFactory = PravegaConfigHelper.getClientFactory(scopeName); //ClientFactory.withScope(scopeName, clientConfig);
        StreamManager streamManager = StreamManager.create(PravegaConfigHelper.getClientConfig());
        sharedStore = PravegaStoreFactory.createSharedStore(clientFactory, streamManager, scopeName, streamName, sorted, storeType);
    }

    protected <T extends PravegaSharedStore> T getStore() {
        return (T) sharedStore;
    }

    @Override
    public void refresh() {
        try {

            sharedStore.refresh();

        } catch (Exception e) {
            log.error("Synchronizer Refresh Exception", e);
            throw e;
        }
    }

    @Override
    public void cleanup() {
        try {
            sharedStore.cleanup();
        } catch (Exception e) {
            log.error("Synchronizer Cleanup Exception", e);
            throw e;
        }
    }

    @Override
    public void close() {
        try {
            sharedStore.close();
        } catch (Exception e) {
            log.error("Synchronizer Remove Exception", e);
            throw e;
        }
    }
}
