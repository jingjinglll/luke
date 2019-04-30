/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.luke.models.commits;

import java.io.IOException;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.store.Directory;

/**
 * Holder for a index file.
 */
public final class File {
  private String fileName;
  private String displaySize;

  static File of(String indexPath, String name) {
    File file = new File();
    file.fileName = name;
    java.io.File fileObject = new java.io.File(indexPath, name);
    if(DirectoryHandler.getInstance().getState().getDirImpl().endsWith("PravegaDirectory")) {
      try {
        file.displaySize = CommitsImpl.toDisplaySize(DirectoryHandler.getInstance().getState().getDirectory().fileLength(name));
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      file.displaySize = CommitsImpl.toDisplaySize(fileObject.length());
    }
    return file;
  }

  public String getFileName() {
    return fileName;
  }

  public String getDisplaySize() {
    return displaySize;
  }

  private File() {
  }
}
