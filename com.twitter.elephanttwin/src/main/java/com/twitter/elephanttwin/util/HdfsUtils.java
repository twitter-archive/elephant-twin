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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.GzipCodec;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.common.base.MorePreconditions;

/**
 * Utilities for dealing with HDFS files.
 *
 * This class needs a lot of TLC to unjank historical accumulated cruft.
 */
public final class HdfsUtils {
  private static final Logger LOG = Logger.getLogger(HdfsUtils.class.getName());

  private static final String HDFS_SCHEME = "hdfs://";

  /**
   * Glob expression for part files.
   */
  public static final String PART_FILE_GLOB = "part-*";

  private HdfsUtils() {
    // utility
  }

  public static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return ! (name.startsWith(".") || name.startsWith("_"));
    }
  };

  /* return visible .lzo files only */
  public final static PathFilter lzoFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return name.endsWith(".lzo") && !name.startsWith(".") && !name.startsWith("_");
    }
  };

  /* filter out hidden directories */
  public final static PathFilter hiddenDirectoryFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith(".") && !name.startsWith("_");
    }
  };

  /**
   * Tests to see if an HDFS cluster is currently reachable.
   *
   * @param hdfsNameNode the namenode to test for reachability
   * @return {@code true} if the name node is reachable.
   */
  public static boolean reachable(String hdfsNamenode) {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsNamenode);
    FileSystem fs;
    try {
      fs = FileSystem.get(config);
      if (fs != null) {
        fs.close();
        return true;
      }
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Fetch {code @fileName} if it matches an HDFS naming convention (hdfs://<namenode>/<path>).
   *
   * @param fileName The name of the file to open.
   * @param localFileName The name of the local file to store to.
   * @return The local file, or null if no HDFS files matched.
   * @throws IOException
   */
  public static File getFile(String fileName, String localFileName) throws IOException {
    // TODO(Jimmy Lin): JohnS suggests something like:
    //    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(configuration))
    // And then just usual URL fetching? WTF uses this to get experiments from HDFS for example.
    // Need to investigate this.
    Preconditions.checkNotNull(localFileName);
    Preconditions.checkNotNull(fileName);
    @Nullable File file = null;
    if (HdfsUtils.isHdfsFile(fileName)) {
      // Separate the HDFS namenode from the rest of the file name.
      Iterator<String> iter  = Splitter.on("/").split(fileName).iterator();
      String hdfsNameNode = iter.next() + "//" + iter.next() + iter.next();
      file = getHdfsFiles(hdfsNameNode, fileName, localFileName);
    }
    return file;
  }

  /**
   * Concatenate the content of all HDFS files matching {@code hdfsGlob} into a
   * file {@code localFilename}.
   *
   * @param hdfsNameNode The name of the Hadoop name node.
   * @param hdfsGlob Files matching this pattern will be fetched.
   * @param localFilename Name of local file to store concatenated content.
   * @return The newly created file.
   * @throws IOException when the file cannot be created/written.
   */
  public static File getHdfsFiles(String hdfsNameNode,
                                  String hdfsGlob,
                                  String localFilename) throws IOException {
    Preconditions.checkNotNull(localFilename);
    Preconditions.checkNotNull(hdfsGlob);
    Preconditions.checkNotNull(hdfsNameNode);
    // init the FS connection and the local file.
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsNameNode);
    FileSystem dfs = FileSystem.get(config);
    File localFile = new File(localFilename);
    FileOutputStream localStream = new FileOutputStream(localFile);

    // get files that need downloading.
    FileStatus[] statuses = dfs.globStatus(new Path(hdfsGlob));
    LOG.info("Pattern " + hdfsGlob + " matched " + statuses.length + " HDFS files, "
             + "fetching to " + localFile.getCanonicalPath() + "...");

    // append each file.
    int copiedChars = 0;
    FSDataInputStream remoteStream = null;
    for (FileStatus status : statuses) {
      Path src = status.getPath();
      try {
        remoteStream = dfs.open(src);
        copiedChars += IOUtils.copy(remoteStream, localStream);
      } catch (IOException e) {
        LOG.severe("Failed to open/copy " + src);
      } finally {
        IOUtils.closeQuietly(remoteStream);
      }
    }
    LOG.info("Fetch " + copiedChars + " bytes to local FS");
    return localFile;
  }

  // TODO(Jimmy Lin): This method is from common_internal.io.HdfsUtil.
  // Does basically the same thing as the ones above. Combine.
  public static File downloadFileFromHdfs(final FileSystem hdfs, final String hdfsFilePath,
      final String localDirName) throws IOException {
    Preconditions.checkNotNull(hdfs);
    MorePreconditions.checkNotBlank(hdfsFilePath);

    File localDir = new File(MorePreconditions.checkNotBlank(localDirName));
    Preconditions.checkArgument(localDir.exists(),
        "Local directory does not exist: " + localDirName);
    Preconditions.checkArgument(localDir.isDirectory(),
        "Not a directory: " + localDirName);

    Path path = new Path(hdfsFilePath);
    FSDataInputStream remoteStream = hdfs.open(path);
    String localFileName = localDirName.endsWith("/") ?
        localDirName + path.getName() : localDirName + "/" + path.getName();
    File localFile = new File(localFileName);
    FileOutputStream localStream = new FileOutputStream(localFile);
    try {
      IOUtils.copy(remoteStream, localStream);
    } finally {
      IOUtils.closeQuietly(remoteStream);
      IOUtils.closeQuietly(localStream);
    }
    return localFile;
  }

  // TODO(Jimmy Lin): This method is from mesos.util.HdfsUtil.
  // Does basically the same thing as the ones above. Combine.
  public static File downloadFileFromHdfs(Configuration conf, String fileUri,
      String localDirName, boolean overwrite) throws IOException {

    Path path = new Path(fileUri);
    FileSystem hdfs = path.getFileSystem(conf);
    FSDataInputStream remoteStream = hdfs.open(path);

    File localFile = new File(localDirName, path.getName());

    if (overwrite && localFile.exists()) {
      boolean success = localFile.delete();
      if (!success) {
        LOG.warning("Failed to delete file to be overwritten: " + localFile);
      }
    }

    FileOutputStream localStream = new FileOutputStream(localFile);
    try {
      IOUtils.copy(remoteStream, localStream);
    } finally {
      IOUtils.closeQuietly(remoteStream);
      IOUtils.closeQuietly(localStream);
    }
    return localFile;
  }

  /**
   * Tests a filename is a fully-qualified hdfs path name; ie: hdfs://...
   *
   * @return true if {@code fileName} matches an HDFS file name.
   */
  public static boolean isHdfsFile(String fileName) {
    return fileName.startsWith(HDFS_SCHEME);
  }

  // TODO(Jimmy Lin): This method does basically the same thing as the above. Refactor to combine.
  public static boolean isValidFile(final FileSystem hdfs, final String path) throws IOException {
    FileStatus[] statuses = hdfs.listStatus(new Path(path));
    return (statuses.length == 1 && !statuses[0].isDir() && statuses[0].getBlockSize() > 0L);
  }

  public static FileSystem getHdfsFileSystem(final String hdfsConfigPath) throws IOException {
    return FileSystem.get(getHdfsConfiguration(hdfsConfigPath));
  }

  public static Configuration getHdfsConfiguration(String hdfsConfigPath) throws IOException {
    Configuration conf = new Configuration(true);
    conf.addResource(new Path(hdfsConfigPath));
    conf.reloadConfiguration();
    return conf;
  }

  /**
   * @param result contains the list of FileStatus passed the filtering conditions;
   * @param fs
   * @param path
   * @param dirFilter : filter works on directories only;
   * @param fileFilter: filer works on files only;
   * @throws IOException
   */
  public static void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter dirFilter, PathFilter fileFilter)
      throws IOException {
    FileStatus[] stats = fs.listStatus(path);
    if (stats != null) {
      for (FileStatus stat : stats) {
        if (stat.isDir() && dirFilter.accept(stat.getPath())) {
          addInputPathRecursively(result, fs, stat.getPath(), dirFilter,
              fileFilter);
        } else {
          if (fileFilter.accept(stat.getPath())) {
            result.add(stat);
          }
        }
      }
    }
  }

  /**
   * Returns {@link FileStatus} instances for all part files beneath the given parent URI.
   *
   * @param fs file system with which to retrieve part file status.
   * @param parent the parent URI within which part files should be globbed.
   * @return status of part files within parent URI.
   * @throws IOException
   */
  public static List<FileStatus> partFileStatus(FileSystem fs, URI parent) throws IOException {
    return Lists.newArrayList(fs.globStatus(new Path(new Path(parent), PART_FILE_GLOB)));
  }

  public static Iterable<Path> getSubdirectories(
      final boolean recursive,
      final String baseDirectory,
      final FileSystem hdfs) throws IOException {

    FileStatus[] fileStat;
    Path basePath = new Path(baseDirectory);
    if(!hdfs.exists(basePath)) {
      throw new IOException(hdfs.getWorkingDirectory() + baseDirectory
                            + " does not exist, cannot getSubdirectories");
    }
    FileStatus status = hdfs.getFileStatus(basePath);
    if(!status.isDir()) {
      LOG.warning("tried to find subdirectories of " + status.getPath() + ", but it is a file");
      return Lists.newArrayList(status.getPath());
    }
    // get the stat on all files in the source directory
    fileStat = hdfs.listStatus(basePath);

    if(fileStat == null) {
      throw new IOException("FileSystem.listStatus(" + basePath
                            + ") returned null, cannot getSubdirectories");
    }

    // get paths to the files in the source directory
    return Arrays.asList(FileUtil.stat2Paths(fileStat));
  }

  /**
   * Copies a directory on fs and its contents recursively to the given local path.
   *
   * @param fs FileSystem, can be hdfs or a local fs
   * @param srcPath The path of the directory whose content you want to copy to local
   * @param localDestPath The directory into which you want to copy the contents.
   */
  public static void copyDirContentsToLocal(FileSystem fs, String srcPath, String localDestPath)
      throws IOException{
    Iterable<Path> indexFiles = HdfsUtils.getSubdirectories(true, srcPath, fs);
    localDestPath = localDestPath.endsWith("/") ? localDestPath : localDestPath + "/";

    for (Path indexFile : indexFiles) {
      LOG.info("Attempting to fetch from hdfs: " + indexFile.toString());
      HdfsUtils.downloadFileFromHdfs(fs, indexFile.toString(), localDestPath);
    }
  }

  /**
   * Returns the namenode from a fully-qualified HDFS path.
   *
   * @param hdfsPath fully-qualified HDFS path
   * @return the namenode or {@code null} if invalid HDFS path
   */
  public static String getHdfsFileSystemName(@Nullable String hdfsPath) {
    if (hdfsPath == null) {
      return null;
    }
    if (!hdfsPath.startsWith("hdfs://")) {
      return null;
    }

    URI uri;
    try {
      uri = new URI(hdfsPath);
    } catch (URISyntaxException e) {
      return null;
    }

    if (uri.getPort() == -1) {
      return "hdfs://" + uri.getHost();
    }
    return "hdfs://" + uri.getHost() + ":" + uri.getPort();
  }

  // This is a helper method. Use openInputStream.
  private static DataInputStream openFile(String file) throws IOException {
    // Open a local file or file on HDFS.
    if (HdfsUtils.isHdfsFile(file)) {
      // Find out what the namenode is, set up the proper configuration, and then
      // open the stream itself.
      Configuration config = new Configuration();
      config.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, getHdfsFileSystemName(file));
      FileSystem fs = FileSystem.get(config);
      // TODO(Jimmy Lin): above three lines are repeated a lot. Refactor to abstract.

      return fs.open(new Path(file));
    } else {
      // Open local file.
      return new DataInputStream(new FileInputStream(new File(file)));
    }
  }

  /**
   * Opens an input stream to a file. Tries to do "the right thing", e.g., if it's a
   * fully-qualified (with proper schema) URI to file from HDFS, will read from HDFS; otherwise,
   * assumes local file; tries to handle compression appropriately, etc.
   *
   * @param dataFile file
   * @return input stream
   * @throws IOException
   */
  public static InputStream openInputStream(String dataFile) throws IOException {
    return getInputStreamSupplier(dataFile).getInput();
  }

  /**
   * Same as {@link openInputStream}, except with a supplier as an extra level of indirection.
   *
   * @param dataFile
   * @return InputSupplier that provides the input stream
   * @throws IOException
   */
  public static InputSupplier<InputStream> getInputStreamSupplier(String dataFile)
      throws IOException {
    Preconditions.checkNotNull(dataFile);

    final InputStream in;
    if (dataFile.endsWith(".lzo")) {
      // Properly handle compressed files.
      LzopCodec codec = new LzopCodec();
      codec.setConf(new Configuration());
      in = codec.createInputStream(openFile(dataFile));
    } else if (dataFile.endsWith(".gz")) {
      GzipCodec codec = new GzipCodec();
      codec.setConf(new Configuration());
      in = codec.createInputStream(openFile(dataFile));
    } else {
      in = openFile(dataFile);
    }

    return new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        // TODO(Jimmy Lin): JohnS noted that this breaks the contract of InputSupplier
        // on 2 counts---the open is not lazy/initiated by the caller of getInput and
        // getInput does not return a fresh stream on each call.
        return in;
      }
    };
  }

  /**
   * Returns a InputSupplier that returns a stream which is the concatenation of all source
   * streams. Note that this method may not make sense for certain binary-encoded streams, but
   * is convenient for processing multiple Hadoop part files in a line-delimited record format.
   *
   * @param streams source streams
   * @return InputSupplier supplying a concatenated stream
   * @throws IOException
   */
  public static InputSupplier<InputStream> getInputStreamSupplier(InputStream... streams)
      throws IOException {
    // TODO(Jimmy Lin): JohnS noted that this breaks the contract of InputSupplier
    // on 2 counts---the open is not lazy/initiated by the caller of getInput and
    // getInput does not return a fresh stream on each call.

    // Furthermore: This only makes sense with InputSupplier<InputStream> inputs and then
    // ByteStreams.join already has this covered.
    return ByteStreams.join(Iterables.transform(Lists.newArrayList(streams),
        new Function<InputStream, InputSupplier<InputStream>>() {
          @Override
          public InputSupplier<InputStream> apply(final InputStream stream) {
            return new InputSupplier<InputStream>() {
              @Override
              public InputStream getInput() throws IOException {
                return stream;
              }
            };
          }
        }));
  }

  /**
   * Returns a InputSupplier that returns a stream which is the concatenation of all source
   * files. Note that this method may not make sense for certain binary-encoded streams, but
   * is convenient for processing multiple Hadoop part files in a line-delimited record format.
   *
   * @param files source files
   * @return InputSupplier supplying a concatenated stream
   * @throws IOException
   */
  public static InputSupplier<InputStream> getInputStreamSupplier(List<String> files)
      throws IOException {
    return ByteStreams.join(Iterables.transform(files,
        new Function<String, InputSupplier<InputStream>>() {
          @Override
          public InputSupplier<InputStream> apply(final String file) {
            return new InputSupplier<InputStream>() {
              @Override
              public InputStream getInput() throws IOException {
                return openInputStream(file);
              }
            };
          }
        }));
  }

  /**
   * Get the last modification date of an HDFS file.
   *
   * @param fs The file system.
   * @param fileUri URI of the file.
   * @return The last modification date of the file, in msecs since epoch, or -1 if unknown.
   * @throws IOException
   */
  public static long getModificationTime(FileSystem fs, String fileUri) throws IOException {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(fileUri);
    FileStatus fileStatus = fs.getFileStatus(new Path(fileUri));
    return fileStatus == null ? -1 : fileStatus.getModificationTime();
  }

  /**
   * @param fs The file system.
   * @param fileUri URI of the file.
   * @return True if the file is an HDFS directory.
   * @throws IOException
   */
  public static boolean isDirectory(FileSystem fs, String fileUri) throws IOException {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(fileUri);
    FileStatus fileStatus = fs.getFileStatus(new Path(fileUri));
    return fileStatus != null && fileStatus.isDir();
  }

  /**
   * Read UTF-8 lines of text directly from HDFS.
   */
  public static Collection<String> readLines(FileSystem fs, Path path)
      throws IOException {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(path);
    LOG.info("Reading from " + path.toUri());
    List<String> lines = Lists.newArrayList();
    try {
      if (!fs.exists(path)) {
        throw new IOException("File not found at " + path);
      }

      // TODO(Jimmy Lin): return CharStreams.readLines(new InputStreamReader(fs.open(path), "UTF-8"))
      // Note that this basically dups the functionality of HdfsFileTransfer.
      BufferedReader stream = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));
      String line;
      while ((line = stream.readLine()) != null) {
        lines.add(line);
      }
      LOG.info("Read " + lines.size() + " queries from " + path.toUri());
      return lines;
    } catch (IOException e) {
      LOG.warning("Failed to read " + path.toUri() + ": " + e.toString());
      throw e;
    }
  }

  /**
   * Write \n separated lines of text to HDFS as UTF-8.
   */
  public static void writeLines(FileSystem fs, Path path, Iterable<String> lines)
      throws IOException {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(lines);
    Writer stream = new BufferedWriter(new OutputStreamWriter(fs.create(path), "UTF-8"));
    try {
      for (String line : lines) {
        stream.write(line + "\n");
      }
    } finally {
      stream.close();
    }
  }

}
