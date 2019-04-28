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
package com.dell.pravegasearch.config;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.context.environment.Environment;
import org.cfg4j.source.context.environment.ImmutableEnvironment;
import org.cfg4j.source.context.filesprovider.ConfigFilesProvider;
import org.cfg4j.source.files.FilesConfigurationSource;
import org.cfg4j.source.reload.ReloadStrategy;
import org.cfg4j.source.reload.strategy.PeriodicalReloadStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(ConfigProvider.class);
    private static final int INTERVAL_SECONDS = 120;
    private String configPath;

    public ConfigProvider(String configPath) {
        if (configPath == null) {
            this.configPath = ConfigPath.getConfigFilePath();
        } else {
            this.configPath = configPath;
        }
        log.info("configPath = {}", this.configPath);
    }

    private ConfigurationProvider configProvider() {
        ConfigFilesProvider configFilesProvider = () -> Collections.singletonList(Paths.get("psearch.yml"));
        // Use local files as configuration store
        ConfigurationSource source = new FilesConfigurationSource(configFilesProvider);

        Environment environment = new ImmutableEnvironment(configPath);

        // Reload configuration
        ReloadStrategy reloadStrategy = new PeriodicalReloadStrategy(INTERVAL_SECONDS, TimeUnit.SECONDS);

        // Create provider
        return new ConfigurationProviderBuilder()
                .withConfigurationSource(source)
                .withEnvironment(environment)
                .withReloadStrategy(reloadStrategy)
                .build();
    }

    public void setupConfig() {
        ConfigurationProvider configurationProvider = configProvider();
//        CommonConfig commonConfig = configurationProvider.bind("psearch", CommonConfig.class);
        PravegaConfig pravegaConfig = configurationProvider.bind("pravega", PravegaConfig.class);
//        ConfigFactory.initConfig(commonConfig);
        ConfigFactory.initConfig(pravegaConfig);
    }
}