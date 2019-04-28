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

import java.io.File;
import java.nio.file.Paths;

public class ConfigPath {

    private static String checkConfigFilePath(String configPath) {
        String configFilePath = Paths.get(configPath).toString();
        File file = new File(configFilePath + Paths.get("/").toString() + "psearch.yml");
        if (file.exists()) {
            return configFilePath;
        }
        return null;
    }

    public static String getConfigFilePath() {
        String configPath;

        String defaultConfigFilePath = "conf";
        configPath = checkConfigFilePath(defaultConfigFilePath);
        if (configPath != null) {
            return configPath;
        }

        String configFilePathForTest = "../config";
        configPath = checkConfigFilePath(configFilePathForTest);
        if (configPath != null) {
            return configPath;
        }

        String configFilePathForIntegrationTest = "../../config";
        configPath = checkConfigFilePath(configFilePathForIntegrationTest);
        if (configPath != null) {
            return configPath;
        }

        String standaloneConfigFilePathInWindows = "config";
        configPath = checkConfigFilePath(standaloneConfigFilePathInWindows);
        if (configPath != null) {
            return configPath;
        }

        throw new IllegalArgumentException("configPath is null.");
    }
}
