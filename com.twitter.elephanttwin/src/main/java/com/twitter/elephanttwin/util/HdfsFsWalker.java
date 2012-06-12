/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.elephanttwin.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HdfsFsWalker {
  private static final Logger LOG = LogManager.getLogger(HdfsFsWalker.class);

  protected final Configuration conf;
  protected final PathFilter pathFilter;

  public HdfsFsWalker(Configuration conf) {
    this(conf, null);
  }

  public HdfsFsWalker(Configuration conf, PathFilter pathFilter) {
    this.conf = conf;
    this.pathFilter = pathFilter;
  }

    /**
   * Walk recursively (depth-first) through the file system beneath path, calling the
   * passed in function on each path.
   *
   * @param path the path at which to start walking
   * @param evalFunc a functional representing the desired action to take on each path
   * @throws IOException
   */
  public void walk(Path path, Functional.F2<Boolean, FileStatus, FileSystem> evalFunc) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(path.toString()), conf);
    if (fs.exists(path) && (pathFilter == null || pathFilter.accept(path))) {
      walkInternal(fs.getFileStatus(path), fs, evalFunc, 0);
    } else {
      LOG.info("Refusing to walk. fs.exists? " + fs.exists(path) + "pathFilter accepts? " + pathFilter.accept(path));
    }
  }

  // The actual implementation, which is semi-careful about files being deleted mid-walk
  // and which does the depth-first traversal.
  private void walkInternal(FileStatus fileStatus, FileSystem fs,
      Functional.F2<Boolean, FileStatus, FileSystem> evalFunc, int nestingLevel) throws IOException {
    if (pathFilter != null && !pathFilter.accept(fileStatus.getPath())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Path Filter did not accept " + fileStatus.getPath() + ", skipping.");
      }
      return;
    }

    // Recursively walk subdirectories
    if (fileStatus.isDir()) {
      FileStatus[] statuses = fs.listStatus(fileStatus.getPath());
      if (statuses != null) {
        for (FileStatus childStatus: statuses) {
          walkInternal(childStatus, fs, evalFunc, nestingLevel + 1);
        }
      }
    }

    // Finally, evaluate the current directory.
    try {
      evalFunc.eval(fileStatus, fs);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }
}
