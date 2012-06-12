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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

import org.apache.pig.impl.util.ObjectSerializer;

/**
 * Closely modeled after Pig Expression class. Serializable.
 */
@SuppressWarnings("serial")
public abstract class Expression implements Serializable {
  // Operator type
  public static  enum OpType  implements Serializable {
    // binary arith ops
    OP_PLUS (" + "),
    OP_MINUS(" - "),
    OP_TIMES(" * "),
    OP_DIV(" / "),
    OP_MOD(" % "),

    //binary ops
    OP_EQ(" == "),
    OP_NE(" != "),
    OP_GT(" > "),
    OP_GE(" >= "),
    OP_LT(" < "),
    OP_LE(" <= "),

    //binary logical
    OP_AND(" and "),
    OP_OR(" or "),
    TERM_COL(" Column "),
    TERM_CONST(" Constant ");

    private String str = "";
    private OpType(String rep){
      this.str = rep;
    }
    private OpType(){
    }

    @Override
    public String toString(){
      return this.str;
    }
  }

  protected OpType opType;

  /**
   * @return the opType
   */
  public OpType getOpType() {
    return opType;
  }




  public static class BinaryExpression extends Expression  implements Serializable {

    /**
     * left hand operand
     */
    Expression lhs;

    /**
     * right hand operand
     */
    Expression rhs;


    /**
     * @param lhs
     * @param rhs
     */
    public BinaryExpression(Expression lhs, Expression rhs, OpType opType) {
      this.opType = opType;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    public BinaryExpression() {
    }

    /**
     * @return the left hand operand
     */
    public Expression getLhs() {
      return lhs;
    }

    /**
     * @return the right hand operand
     */
    public Expression getRhs() {
      return rhs;
    }

    @Override
    public String toString() {
      return "(" + lhs.toString() + opType.toString() + rhs.toString()
          + ")";
    }
  }

  public static class Column extends Expression implements Serializable {
    /**
     * name of column
     */
    private String name;

    /**
     * @param name
     */
    public Column(String name) {
      this.opType = OpType.TERM_COL;
      this.name = name;
    }
    public Column() {
    }

    @Override
    public String toString() {
      return name;
    }

    /**
     * @return the name
     */
    public String getName() {
      return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
      this.name = name;
    }
  }

  public static class Const extends Expression {

    /**
     * value of the constant
     */
    Object value;

    /**
     * @return the value
     */
    public Object getValue() {
      return value;
    }
    public Const() {
    }
    /**
     * @param value
     */
    public Const(Object value) {
      this.opType = OpType.TERM_CONST;
      this.value = value;
    }

    @Override
    public String toString() {
      return (value instanceof String) ? "\'" + value + "\'":
        value.toString();
    }
  }
  /**
   * support only OR, AND operators. Each "leaf level" expression must be equality condition ("==")
   * like Column1 == 'constant'
   * @param partitionFilter
   * @return true if the filter conditions are supported by this loader
   */
  public static boolean isSupported(org.apache.pig.Expression partitionFilter) {
    if (! (partitionFilter instanceof org.apache.pig.Expression.BinaryExpression))
      return false;

    org.apache.pig.Expression.BinaryExpression be = (org.apache.pig.Expression.BinaryExpression) partitionFilter;
    org.apache.pig.Expression rhs=be.getRhs();
    org.apache.pig.Expression  lhs=be.getLhs();

    if( be.getOpType() ==   org.apache.pig.Expression.OpType.OP_EQ) {    //"leaf node"
      //handle cases like 'abcd' == column , column == 'abcd'
      if( rhs instanceof org.apache.pig.Expression.Column && lhs instanceof org.apache.pig.Expression.Const)
        return ((org.apache.pig.Expression.Const)(lhs)).getValue() instanceof String;
      else if (lhs instanceof org.apache.pig.Expression.Column && rhs instanceof org.apache.pig.Expression.Const)
        return  ((org.apache.pig.Expression.Const)(rhs)).getValue() instanceof String;
      else
        return false;
    }
    else if (be.getOpType() == org.apache.pig.Expression.OpType.OP_OR ||
        be.getOpType() == org.apache.pig.Expression.OpType.OP_AND)
      return (isSupported(rhs) && isSupported(lhs));
    else
      return false;
  }
  /**
   *Translate pig.Expression to a Expression which implements Serializable
   *to support filter condition pushdown to MR RecordReader,
   *before we can modify pig.Expression and commit it to the main branch.
   */
  public static BinaryExpression newInstance(org.apache.pig.Expression expression) throws IOException {

    if(!Expression.isSupported(expression))
      throw new IOException("not supported pig Expression type " + expression);

    org.apache.pig.Expression.BinaryExpression filter = (org.apache.pig.Expression.BinaryExpression) expression;
    org.apache.pig.Expression rhs = filter.getRhs();
    org.apache.pig.Expression lhs = filter.getLhs();

    if( filter.getOpType() == org.apache.pig.Expression.OpType.OP_EQ) {   //"leaf node"
      if( rhs instanceof org.apache.pig.Expression.Column) {
        lhs = filter.getRhs();
        rhs = filter.getLhs();
      }
      String value = (String) ((org.apache.pig.Expression.Const)rhs).getValue();
      Const c = new Const(value);
      return  new BinaryExpression(new Column( ((org.apache.pig.Expression.Column)lhs).getName()),
          c, OpType.OP_EQ);
    }

    // now be.getOpType() is OR or AND)
    BinaryExpression newLhs = newInstance( lhs);
    BinaryExpression newRhs = newInstance( rhs);

    if( filter.getOpType() == org.apache.pig.Expression.OpType.OP_OR)
      return new BinaryExpression(newLhs, newRhs, OpType.OP_OR);
    else
      return new BinaryExpression(newLhs, newRhs, OpType.OP_AND);
  }
  /**
   * @param filterString
   * @return return deserialized filter condition.
   * @throws IOException
   */
  public static BinaryExpression getFilterCondition(String filterConditions)
      throws IOException {
    BinaryExpression filter = null;
    if (filterConditions == null || filterConditions.length() == 0)
      filter = null;
    else {
      try {
        ByteArrayInputStream serialObj = new ByteArrayInputStream(
            ObjectSerializer.decodeBytes(filterConditions));
        ObjectInputStream objStream = new ObjectInputStream(serialObj) {
          /** while waiting for PIG's fix/patch for resolving issues related
           * to PIG-2532.  Resolve classes using both the root class loader
           * (classes on the java classpath).
           * and context class loader (registered jars). This is necessary
           * when registered classes have been serialized.
           */
          @Override
          protected Class<?> resolveClass(ObjectStreamClass objectStreamClass)
              throws IOException, ClassNotFoundException {
            try {
              return super.resolveClass(objectStreamClass);
            } catch (ClassNotFoundException e) {
              return Class.forName(objectStreamClass.getName(), false, Thread
                  .currentThread().getContextClassLoader());
            }
          }
        };
        filter = (BinaryExpression) objStream.readObject();
      } catch (Exception e) {
        throw new IOException("Deserialization error: " + e.getMessage(), e);
      }
    }
    return filter;
  }
}