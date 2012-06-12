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

import com.google.common.collect.Maps;
import org.apache.lucene.document.FieldType;

import java.util.Map;

public class LuceneUtil {

  public static final int STORED_NO_INDEX_NO_NORM_NO_TOKEN = 0;
  public static final int NO_STORE_INDEX_NO_NORM_TOKEN = 1;
  public static final int NO_STORE_INDEX_NORM_TOKEN = 2;
  private static final Map<Integer, CustomFieldType> customFieldTypeMap = Maps.newHashMap();

  static {
    for(CustomFieldType cft : CustomFieldType.values())  {
      customFieldTypeMap.put(cft.ordinal(), cft);
    }
  }

  public static FieldType getFieldType(Integer typeId) {
    if (typeId != null && customFieldTypeMap.get(typeId) != null)  {
      return customFieldTypeMap.get(typeId).get();
    }
    return null;
  }

  public enum CustomFieldType {
    STORED_NO_INDEX_NO_NORM_NO_TOKEN {    //ordinal = 0
      @Override
      FieldType get() {
        FieldType fieldType = new FieldType();
        fieldType.setStored(true);
        fieldType.setIndexed(false);
        fieldType.setOmitNorms(true);
        fieldType.setTokenized(false);
        return fieldType;
      } },
    NO_STORE_INDEX_NO_NORM_TOKEN {        //ordinal = 1
      @Override
      FieldType get() {
        FieldType fieldType = new FieldType();
        fieldType.setStored(false);
        fieldType.setIndexed(true);
        fieldType.setOmitNorms(true);
        fieldType.setTokenized(true);
        return fieldType;
      } },
    NO_STORE_INDEX_NORM_TOKEN {           //ordinal = 2
      @Override
      FieldType get() {
        FieldType fieldType = new FieldType();
        fieldType.setStored(false);
        fieldType.setIndexed(true);
        fieldType.setOmitNorms(false);
        fieldType.setTokenized(true);
        return fieldType;
      } };
    abstract FieldType get();
  }
}
