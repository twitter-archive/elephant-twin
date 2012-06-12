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
package com.twitter.elephanttwin.retrieval;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.twitter.elephanttwin.util.HdfsUtils;

/**
 * An abstraction for managing a collection of indexes in YYYY/MM/DD subdirectories under a base
 * directory.
 *
 * @author jimmy
 */
public class StatusesIndexManager {
  private static final Logger LOG = Logger.getLogger(StatusesIndexManager.class);

  private final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

  // A mapping from a day to a list of indexes for that day.
  private final SortedMap<String, List<Path>> mapping = new TreeMap<String, List<Path>>();
  private final FileSystem fs;

  /**
   * Creates a <code>StatusesIndexManager</code>
   *
   * @param baseDir the base directory
   * @param fs handle to the FileSystem
   * @param startDate start date
   * @param endDate end date
   * @throws IOException
   */
  public StatusesIndexManager(Path baseDir, FileSystem fs, Calendar startDate, Calendar endDate)
      throws IOException {

    // copy the Calendar objects (they are mutable and we don't want to mess with the originals)
    // and clear their time related fields, we only care about day resolution
    startDate = (Calendar) startDate.clone();
    startDate.set(Calendar.HOUR_OF_DAY, 0);
    startDate.set(Calendar.MINUTE, 0);
    startDate.set(Calendar.SECOND, 0);
    startDate.set(Calendar.MILLISECOND, 0);
    endDate = (Calendar) endDate.clone();
    endDate.set(Calendar.HOUR_OF_DAY, 0);
    endDate.set(Calendar.MINUTE, 0);
    endDate.set(Calendar.SECOND, 0);
    endDate.set(Calendar.MILLISECOND, 0);

    // Start date should be before or equal to the end date.
    Preconditions.checkArgument(startDate.compareTo(endDate) <= 0);
    this.fs = Preconditions.checkNotNull(fs);

    do {
      String key = dateFormat.format(startDate.getTime());
      // Under directory for each day, there should be a directory for each shard.
      Path pattern = new Path(baseDir, key + "/*");

      // Remember to exclude _log directories. Using a regex here saves us from having to check of
      // empty directories that only contain _log directories, simplifying checks below.
      FileStatus[] statuses = fs.globStatus(pattern, new RegexExcludePathFilter("^.*_logs$"));
      if (statuses == null || statuses.length == 0) {
        // Warn if we can't find directories for that day.
        LOG.warn("Index not found: " + pattern);
      } else {
        for (FileStatus status : statuses) {
          if (!HdfsUtils.HIDDEN_FILE_FILTER.accept(status.getPath())) {
            continue;
          }
          LOG.debug("Index added: " + status.getPath().toString());

          if (mapping.containsKey(key)) {
            mapping.get(key).add(status.getPath());
          } else {
            mapping.put(key, Lists.newArrayList(status.getPath()));
          }
        }
      }

      // Roll forward the calendar to the next day.
      startDate.add(Calendar.DATE, 1);
    } while (startDate.compareTo(endDate) <= 0);
  }

  /**
   * Returns a (unsorted) list of paths to index directories corresponding to key, which is a String
   * in <code>YYYY/MM/DD</code> format.
   */
  public List<Path> getIndexPaths(String key) {
    return mapping.get(key);
  }

  /**
   * Returns a handle to the FileSystem.
   */
  public FileSystem getFileSystem() {
    return fs;
  }

  /**
   * Returns a set of keys associated with this index manager, sorted in ascending temporal order.
   */
  public Set<String> getKeys() {
    return mapping.keySet();
  }

  // This is from Tom White's Hadoop book.
  private static class RegexExcludePathFilter implements PathFilter {
    private final String regex;
    public RegexExcludePathFilter(String regex) {
      this.regex = Preconditions.checkNotNull(regex);
    }

    public boolean accept(Path path) {
      return !path.toString().matches(regex);
    }
  }
}
