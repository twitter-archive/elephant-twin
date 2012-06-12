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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.twitter.common.args.CmdLine;

/**
 * Reflection utilities
 * @author Dmitriy Ryaboy
 */
public final class ReflectionHelper {
  private static final Logger LOG = LogManager.getLogger(ReflectionHelper.class);
  private static final String UNABLE_TO_CREATE_CLASS = "Unable to create class ";
  private static final String UNABLE_TO_FIND_CONSTRUCTOR = "Unable to find cunstructor for ";
  private static final String DOT = "."; // (Alex Levenson): I... I'm sorry... blame checkstyle

  private ReflectionHelper() {
  }

  /**
   * Given a class name and the Class, return an instance of that class
   * instantiated using a no-argument constructor.
   * Returns null if ClassNotFound, Instantiation, or IllegalAccess exceptions are encountered.
   * @param <T>
   * @param className
   * @param klass
   * @return
   */
  public static <T> T createClassFromName(String className, Class<T> klass) {
    try {
      @SuppressWarnings("unchecked")
      Class<T> objClass = (Class<T>) getAnyClassByName(className);
      return objClass.newInstance();
    } catch (ClassNotFoundException e) {
      LOG.warn(UNABLE_TO_CREATE_CLASS + className, e);
    } catch (InstantiationException e) {
      LOG.warn(UNABLE_TO_CREATE_CLASS + className, e);
    } catch (IllegalAccessException e) {
      LOG.warn(UNABLE_TO_CREATE_CLASS + className, e);
    }
    return null;
  }

  /**
   * Tries both the normal Class.forName method, and taking apart the class name to check for
   * a possible inner class (so com.twitter.data.proto.User returns the class
   * com.twitter.data.proto$User)
   * @param className
   * @return
   * @throws ClassNotFoundException
   */
  public static Class<?> getAnyClassByName(String className) throws ClassNotFoundException {
    Class<?> klass = null;
    try {
      klass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      // the class name might be canonical name.
      klass = getInnerClass(className);
    }
    return klass;
  }

  /**
   * Returns the constructor with specific parameters for a class.
   * @param className name of the class
   * @param typeParams type parameters of the constructor
   * @return constructor of the class
   */
  // SUPPRESS CHECKSTYLE return count
  public static Constructor<?> getConstructorForClass(String className, Class<?>[] typeParams) {
    Class<?> klass = null;
    try {
      klass = getAnyClassByName(className);
    } catch (ClassNotFoundException e) {
      LOG.warn("Unable to get class " + className, e);
      return null;
    }

    try {
      return klass.getConstructor(typeParams);
    } catch (SecurityException e) {
      LOG.warn(UNABLE_TO_FIND_CONSTRUCTOR + className, e);
      return null;
    } catch (NoSuchMethodException e) {
      LOG.warn(UNABLE_TO_FIND_CONSTRUCTOR + className, e);
      return null;
    }
  }

  /**
   * get an inner class given a canonical class name
   * @param canonicalClassName
   * @return
   * @throws ClassNotFoundException
   */
  public static Class<?> getInnerClass(String canonicalClassName) throws ClassNotFoundException {
    // is an inner class and is not visible from the outside. We have to instantiate
    String parentClass = canonicalClassName.substring(0, canonicalClassName.lastIndexOf(DOT));
    String subclass = canonicalClassName.substring(canonicalClassName.lastIndexOf(DOT) + 1);
    return getInnerClass(parentClass, subclass);
  }

  /** Get an inner class given a canonical parent name
   * @param canonicalParentName
   * @param subclassName
   * @return
   * @throws ClassNotFoundException
   */
  public static Class<?> getInnerClass(String canonicalParentName, String subclassName)
      throws ClassNotFoundException {
    Class<?> outerClass = Class.forName(canonicalParentName);
    for (Class<?> innerClass : outerClass.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals(subclassName)) {
        return innerClass;
      }
    }
    return null;
  }

  /**
   * Given an object that has fields annotated with CmdLine, and a field name,
   * return the name of the CmdLine argument (meaning, the actual command-line argument one would supply to
   * set this field).
   * @param container  the object that contains an Option
   * @param fieldName  name of the field
   * @return name of the option
   * @throws SecurityException
   * @throws NoSuchFieldException
   */
  public static String nameForOption(Object container, String fieldName)
      throws NoSuchFieldException {
    Field field = container.getClass().getField(fieldName);
    return field.getAnnotation(CmdLine.class).name();
  }
}
