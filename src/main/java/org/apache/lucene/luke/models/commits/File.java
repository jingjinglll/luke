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

import com.dell.pravegasearch.PravegaDirectory;
import java.io.IOException;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.IndexHandler;

/**
 * Holder for a index file.
 */
public final class File {
  private String fileName;
  private String displaySize;

  static File of(String indexPath, String name) {
    File file = new File();
    file.fileName = name;
    boolean isPravegaDirectory = false;
    long length = 0;
    try {
      isPravegaDirectory = IndexHandler.getInstance().getState().getDirImpl().endsWith("PravegaDirectory");
      length = IndexHandler.getInstance().getState().getDirectory().fileLength(name);
    } catch (Exception e) {

    }
    try {
      if(!isPravegaDirectory) {
        isPravegaDirectory = DirectoryHandler.getInstance().getState().getDirImpl().endsWith("PravegaDirectory");
        length = DirectoryHandler.getInstance().getState().getDirectory().fileLength(name);
      }
    } catch (Exception e) {

    }
    if(isPravegaDirectory) {
      file.displaySize = CommitsImpl.toDisplaySize(length);
    } else {
      java.io.File fileObject = new java.io.File(indexPath, name);
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
