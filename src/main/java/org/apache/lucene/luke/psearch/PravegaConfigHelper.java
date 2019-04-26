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

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.DefaultCredentials;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaConfigHelper {
    private static final Logger log = LoggerFactory.getLogger(PravegaConfigHelper.class);

    private static final Map<String, ClientFactory> FACTORY_MAP = new ConcurrentHashMap<>();
    private static final ClientConfig CLIENT_CONFIG;

    static {
        URI controllerUri;
        try {
            controllerUri = new URI(getControllerEndPoint());
        } catch (Exception e) {
            throw new RuntimeException("Fail to get URI for Pravega controller.", e);
        }

        CLIENT_CONFIG = ClientConfig.builder()
                                    .controllerURI(controllerUri)
                                    .credentials(new DefaultCredentials(getPassword(), getUserName()))
                                    .trustStore(getCertFilePem())
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

    public static String getPassword() {
        return ConfigFactory.getPravegaConfig().pravegaPassword();
    }

    public static String getUserName() {
        return ConfigFactory.getPravegaConfig().pravegaUserName();
    }

    public static String getCertFilePem() {
        return fileToString(ConfigFactory.getPravegaConfig().pravegaCertFilePem());
    }

    public static String getKeyFilePem() {
        return fileToString(ConfigFactory.getPravegaConfig().pravegaKeyFilePem());
    }

    public static String getPasswdFilePem() {
        return fileToString(ConfigFactory.getPravegaConfig().pravegaPasswdFilePem());
    }

    public static String getControllerEndPoint() {
       String pravegaControllerAddress = ConfigFactory.getPravegaConfig().pravegaControllerAddress();
       String pravegaControllerPort = ConfigFactory.getPravegaConfig().pravegaControllerPort();

       return "tcp://" + pravegaControllerAddress + ":" + pravegaControllerPort;
    }

    private static String fileToString(String fileName) {
        String result = "";
        InputStream fileStream = PravegaConfigHelper.class.getClassLoader().getResourceAsStream(fileName);
        try {
            result = IOUtils.toString(fileStream, "utf-8");
        } catch (IOException e) {
            log.error("IOEexception {}", e);
        }
        return result;
    }
}