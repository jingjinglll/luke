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

package com.dell.pravegasearch;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.DefaultCredentials;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaConfigHelper {
    private static final Logger log = LoggerFactory.getLogger(PravegaConfigHelper.class);

    private static final Map<String, ClientFactory> FACTORY_MAP = new ConcurrentHashMap<>();
    private static final ClientConfig CLIENT_CONFIG;

    static {
        URI controllerUri;
        try {
            controllerUri = new URI("tcp://127.0.0.1:9090");
        } catch (Exception e) {
            throw new RuntimeException("Fail to get URI for Pravega controller.", e);
        }

        CLIENT_CONFIG = ClientConfig.builder()
                                    .controllerURI(controllerUri)
                                    .credentials(new DefaultCredentials("password", "admin"))
                                    .trustStore("pravegadummycerts/pravegacert.pem")
                                    .validateHostName(false)
                                    .build();
    }

    private PravegaConfigHelper() {
    }

    public static ClientFactory getClientFactory(String scope) {
        log.info("Total client factories: {}", FACTORY_MAP.size());
        return FACTORY_MAP.computeIfAbsent(scope, x -> ClientFactory.withScope(scope, CLIENT_CONFIG));
    }

    public static ClientConfig getClientConfig() {
        return CLIENT_CONFIG;
    }

}