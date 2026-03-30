/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.math.BigDecimal;

import static org.apache.calcite.linq4j.tree.Types.RecordType;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link org.apache.calcite.jdbc.JavaTypeFactoryImpl}.
 */
public final class JavaTypeFactoryTest {
  private static final JavaTypeFactoryImpl TYPE_FACTORY = new JavaTypeFactoryImpl();

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test void testGetJavaClassWithOneFieldStructDataTypeV1() {
    RelDataType structWithOneField = TYPE_FACTORY.createStructType(OneFieldStruct.class);
    assertThat(TYPE_FACTORY.getJavaClass(structWithOneField),
        is(OneFieldStruct.class));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test void testGetJavaClassWithOneFieldStructDataTypeV2() {
    RelDataType structWithOneField =
        TYPE_FACTORY.createStructType(
            ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)),
            ImmutableList.of("intField"));
    assertRecordType(TYPE_FACTORY.getJavaClass(structWithOneField));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test void testGetJavaClassWithTwoFieldsStructDataType() {
    RelDataType structWithTwoFields = TYPE_FACTORY.createStructType(TwoFieldStruct.class);
    assertThat(TYPE_FACTORY.getJavaClass(structWithTwoFields),
        is(TwoFieldStruct.class));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test void testGetJavaClassWithTwoFieldsStructDataTypeV2() {
    RelDataType structWithTwoFields =
        TYPE_FACTORY.createStructType(
            ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)),
        ImmutableList.of("intField", "strField"));
    assertRecordType(TYPE_FACTORY.getJavaClass(structWithTwoFields));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3029">[CALCITE-3029]
   * Java-oriented field type is wrongly forced to be NOT NULL after being converted to
   * SQL-oriented</a>. */
  @Test void testFieldNullabilityAfterConvertingToSqlStructType() {
    RelDataType javaStructType =
        TYPE_FACTORY.createStructType(
            ImmutableList.of(TYPE_FACTORY.createJavaType(Integer.class),
                TYPE_FACTORY.createJavaType(int.class)),
        ImmutableList.of("a", "b"));
    RelDataType sqlStructType = TYPE_FACTORY.toSql(javaStructType);
    assertThat(SqlTests.getTypeString(sqlStructType),
        is("RecordType(INTEGER a, INTEGER NOT NULL b) NOT NULL"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6752">[CALCITE-6752]
   * JavaTypeFactoryImpl cannot represent fractional seconds</a>. */
  @Test void testGetJavaClassWithFractionalSecondIntervalType() {
    final RelDataType intervalSecondType =
        TYPE_FACTORY.createSqlIntervalType(
            new SqlIntervalQualifier(TimeUnit.SECOND, 2, null, 6,
                SqlParserPos.ZERO));
    final RelDataType intervalMinuteSecondType =
        TYPE_FACTORY.createSqlIntervalType(
            new SqlIntervalQualifier(TimeUnit.MINUTE, 2, TimeUnit.SECOND, 6,
                SqlParserPos.ZERO));
    final RelDataType intervalDaySecondType =
        TYPE_FACTORY.createSqlIntervalType(
            new SqlIntervalQualifier(TimeUnit.DAY, 2, TimeUnit.SECOND, 6,
                SqlParserPos.ZERO));
    final RelDataType intervalMinuteType =
        TYPE_FACTORY.createSqlIntervalType(
            new SqlIntervalQualifier(TimeUnit.MINUTE, null, SqlParserPos.ZERO));

    assertThat(TYPE_FACTORY.getJavaClass(intervalSecondType), is((Type) BigDecimal.class));
    assertThat(TYPE_FACTORY.getJavaClass(intervalMinuteSecondType),
        is((Type) BigDecimal.class));
    assertThat(TYPE_FACTORY.getJavaClass(intervalDaySecondType),
        is((Type) BigDecimal.class));
    assertThat(TYPE_FACTORY.getJavaClass(intervalMinuteType), is((Type) long.class));
  }

  private void assertRecordType(Type actual) {
    assertTrue(actual instanceof RecordType,
        () -> "Type {" + actual.getTypeName() + "} is not a subtype of Types.RecordType");
  }

  /** Struct with one field. */
  private static class OneFieldStruct {
    public Integer intField;
  }

  /** Struct with two fields. */
  private static class TwoFieldStruct {
    public Integer intField;
    public String strField;
  }
}
