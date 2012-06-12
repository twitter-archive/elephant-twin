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

import java.util.Iterator;

import com.twitter.elephantbird.util.TypeRef;

public class Functional {

  // Static methods only.
  private Functional() {}

  // Functor interfaces.
  public static interface F0<R> {
    R eval();
  }

  public static interface F1<R, P1> {
    R eval(P1 param1);
  }

  public static interface F2<R, P1, P2> {
    R eval(P1 param1, P2 param2);
  }

  public static interface F3<R, P1, P2, P3> {
    R eval(P1 param1, P2 param2, P3 param3);
  }

  public static interface F4<R, P1, P2, P3, P4> {
    R eval(P1 param1, P2 param2, P3 param3, P4 param4);
  }

  // Functional routines.
  public static <R, P1> Iterator<R> map(final F1<R, P1> fn, final Iterable<P1> iterable) {
    return map(fn, iterable.iterator());
  }

  public static <R, P1> Iterator<R> map(final F1<R, P1> fn, final Iterator<P1> it) {
    return new Iterator<R>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public R next() {
        return fn.eval(it.next());
      }

      @Override
      public void remove() {
        it.remove();
      }
    };
  }

  public static <R, P1, P2> Iterator<R> map(final F2<R, P1, P2> fn,
      final Iterable<P1> iterable1,
      final Iterable<P2> iterable2) {
    return map(fn, iterable1.iterator(), iterable2.iterator());
  }

  public static <R, P1, P2> Iterator<R> map(final F2<R, P1, P2> fn, final Iterator<P1> it1, final Iterator<P2> it2) {
    return new Iterator<R>() {
      @Override
      public boolean hasNext() {
        return it2.hasNext() || it2.hasNext();
      }

      @Override
      public R next() {
        return fn.eval(it1.hasNext() ? it1.next() : null, it2.hasNext() ? it2.next() : null);
      }

      @Override
      public void remove() {
        it1.remove();
        it2.remove();
      }
    };
  }

  public static <R, P> R reduce(final F2<R, R, P> fn, final R initial, final Iterable<P> iterable) {
    return reduce(fn, initial, iterable.iterator());
  }

  public static <R, P> R reduce(final F2<R, R, P> fn, final R initial, final Iterator<P> it) {
    R val = initial;
    while (it.hasNext()) {
      val = fn.eval(val, it.next());
    }
    return val;
  }

  public static <T> F0<T> id(final T v) {
    return new F0<T>() {
      @Override
      public T eval() {
        return v;
      }
    };
  }

  public static <T, P1> F1<T, P1> id1(final T v, final TypeRef<P1> type1) {
    return new F1<T, P1>() {
      @Override
      public T eval(P1 p1) {
        return v;
      }
    };
  }

  // F0-returning curry functions.
  public static <R, P1> F0<R> curry(final F1<R, P1> f1, final P1 param1) {
    return new F0<R>() {
      @Override
      public R eval() {
        return f1.eval(param1);
      }
    };
  }

  public static <R, P1, P2> F0<R> curry(final F2<R, P1, P2> f2, final P1 param1, final P2 param2) {
    return new F0<R>() {
      @Override
      public R eval() {
        return f2.eval(param1, param2);
      }
    };
  }

  public static <R, P1, P2, P3> F0<R> curry(final F3<R, P1, P2, P3> f3,
      final P1 param1, final P2 param2, final P3 param3) {
    return new F0<R>() {
      @Override
      public R eval() {
        return f3.eval(param1, param2, param3);
      }
    };
  }

  public static <R, P1, P2, P3, P4> F0<R> curry(final F4<R, P1, P2, P3, P4> f4,
      final P1 param1, final P2 param2, final P3 param3, final P4 param4) {
    return new F0<R>() {
      @Override
      public R eval() {
        return f4.eval(param1, param2, param3, param4);
      }
    };
  }

  // F1-returning curry functions.
  public static <R, P1, P2> F1<R, P2> curry(final F2<R, P1, P2> f2, final P1 param1) {
    return new F1<R, P2>() {
      @Override
      public R eval(P2 param2) {
        return f2.eval(param1, param2);
      }
    };
  }

  public static <R, P1, P2, P3> F1<R, P3> curry(final F3<R, P1, P2, P3> f3, final P1 param1, final P2 param2) {
    return new F1<R, P3>() {
      @Override
      public R eval(P3 param3) {
        return f3.eval(param1, param2, param3);
      }
    };
  }

  public static <R, P1, P2, P3, P4> F1<R, P4> curry(final F4<R, P1, P2, P3, P4> f4,
      final P1 param1, final P2 param2, final P3 param3) {
    return new F1<R, P4>() {
      @Override
      public R eval(P4 param4) {
        return f4.eval(param1, param2, param3, param4);
      }
    };
  }

  // F2-returning curry functions.
  public static <R, P1, P2, P3> F2<R, P2, P3> curry(final F3<R, P1, P2, P3> f3, final P1 param1) {
    return new F2<R, P2, P3>() {
      @Override
      public R eval(P2 param2, P3 param3) {
        return f3.eval(param1, param2, param3);
      }
    };
  }

  public static <R, P1, P2, P3, P4> F2<R, P3, P4> curry(final F4<R, P1, P2, P3, P4> f4,
      final P1 param1, final P2 param2) {
    return new F2<R, P3, P4>() {
      @Override
      public R eval(P3 param3, P4 param4) {
        return f4.eval(param1, param2, param3, param4);
      }
    };
  }

  // F3-returning curry functions.
  public static <R, P1, P2, P3, P4> F3<R, P2, P3, P4> curry(final F4<R, P1, P2, P3, P4> f4, final P1 param1) {
    return new F3<R, P2, P3, P4>() {
      @Override
      public R eval(P2 param2, P3 param3, P4 param4) {
        return f4.eval(param1, param2, param3, param4);
      }
    };
  }
}
