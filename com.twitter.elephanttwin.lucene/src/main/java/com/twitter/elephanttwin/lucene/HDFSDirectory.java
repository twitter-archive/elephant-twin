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
package com.twitter.elephanttwin.lucene;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.google.common.base.Preconditions;

/**
 * Implementation of a Lucene Directory for reading indexes directly off HDFS.
 *
 * @author jimmy
 */
public class HDFSDirectory extends Directory {
  private final FileSystem fs;
  private final Path dir;

  public HDFSDirectory(String name, FileSystem fs) {
    this.fs = Preconditions.checkNotNull(fs);
    dir = new Path(name);
  }

  public HDFSDirectory(Path path, FileSystem fs) {
    this.fs = Preconditions.checkNotNull(fs);
    dir = path;
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }

  @Override
  public void deleteFile(String name) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return fs.exists(new Path(dir, name));
  }

  @Override
  public long fileLength(String name) throws IOException {
    return fs.getFileStatus(new Path(dir, name)).getLen();
  }

  @Override
  public long fileModified(String name) throws IOException {
    return fs.getFileStatus(new Path(dir, name)).getModificationTime();
  }

  @Override
  public String[] listAll() throws IOException {
    FileStatus[] statuses = fs.listStatus(dir);
    String[] files = new String[statuses.length];

    for (int i = 0; i < statuses.length; i++) {
      files[i] = statuses[i].getPath().getName();
    }
    return files;
  }

  //TODO: IOContext is completely ignored. Make sure there is no side effect.
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    // I'm finding that I need to uncomment this from time to time for debugging:
    //System.out.println("Trying to openInput: " + name);
    return new HDFSIndexInput(new Path(dir, name).toString());
  }

  /**
   * Ensure that any writes to these files are moved to
   * stable storage.  Lucene uses this to properly commit
   * changes to the index, to prevent a machine/OS crash
   * from corrupting the index.<br/>
   * <br/>
   * NOTE: Clients may call this method for same files over
   * and over again, so some impls might optimize for that.
   * For other impls the operation can be a noop, for various
   * reasons.
   */
  //TODO real implementation
  @Override
  public void sync(Collection<String> names) throws IOException {

  }

  //TODO: IOContext is completely ignored. Make sure there is no side effect.
  /** Creates an IndexOutput for the file with the given name. */
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return null;
  }



  private class HDFSIndexInput extends IndexInput {
    private Path path;
    private final FSDataInputStream in;

    protected HDFSIndexInput(String resourceDescription) throws IOException {
      super(resourceDescription);
      path = new Path(resourceDescription);
      this.in = fs.open(path);
    }

    @Override
    public void close() throws IOException {
      // TODO(jimmy): For some reason, if we actually close the stream here, it doesn't work...
      // something having to do how Lucene internally calls this method. Need ask Michael Busch.
    }

    @Override
    public long getFilePointer() {
      try {
        return in.getPos();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long length() {
      try {
        return fs.getFileStatus(path).getLen();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public byte readByte() throws IOException {
      return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      // I'm finding that I need to uncomment this from time to time for debugging:
      // System.out.println("attempt to read " + len + " from " + getFilePointer());

      // Important: use readFully instead of read.
      in.readFully(b, offset, len);
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(pos);
    }
  }

}
