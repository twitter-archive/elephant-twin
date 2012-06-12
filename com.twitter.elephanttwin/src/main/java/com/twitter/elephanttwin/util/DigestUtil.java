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
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DigestUtil {
  private static final int STREAM_BUFFER_LENGTH = 16384;

  // TODO: use commons-codec for this instead.

  /**
   * Compute the digest for a given hash over the input stream.
   * @param digest the algorithm to use.
   * @param in the input stream
   * @return the digest in byte array form.
   * @throws IOException
   */
  public static byte[] getDigest(MessageDigest digest, InputStream in) throws IOException {
    byte[] buffer = new byte[STREAM_BUFFER_LENGTH];
    int ret = in.read(buffer, 0, STREAM_BUFFER_LENGTH);

    while (ret != -1) {
      digest.update(buffer, 0, ret);
      ret = in.read(buffer, 0, STREAM_BUFFER_LENGTH);
    }

    return digest.digest();
  }

  /**
   * Compute the digest in hex-encoded form.
   * @param digest the hash algorithm to use
   * @param in the input stream
   * @return the hex-encoded digest
   * @throws IOException
   */
  public static String getDigestAsString(MessageDigest digest, InputStream in) throws IOException {
    return HexCodec.toHexString(getDigest(digest, in));
  }

  /**
   * Get the md5 digest in hex-encoded form
   * @param in the input stream
   * @return the hex-encoded digest
   * @throws IOException
   */
  public static String getMd5DigestAsString(InputStream in) throws IOException {
    return getDigestAsString(getMd5Digest(), in);
  }

  /**
   * Get the raw md5 digest of the given input stream
   * @param in the input stream
   * @return the digest of the exhausted stream, as a byte array
   * @throws IOException
   */
  public static byte[] getMd5Digest(InputStream in) throws IOException {
    return getDigest(getMd5Digest(), in);
  }

  /**
   * Get the md5 digest
   * @return a MD5 message digest
   */
  protected static MessageDigest getMd5Digest() {
    return getDigest("MD5");
  }

  /**
   * Get a digest for a generic algorithm, and rethrow an unchecked exception on failure
   * @param algorithm the algorithm to use, e.g. MD5
   * @return a message digest for the given algorithm
   */
  protected static MessageDigest getDigest(String algorithm) {
    try {
      return MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
