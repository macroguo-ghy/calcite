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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link RexToLixTranslator}.
 */
public class RexToLixTranslatorTest {
  private static final JavaTypeFactoryImpl TYPE_FACTORY = new JavaTypeFactoryImpl();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6752">[CALCITE-6752]
   * JavaTypeFactoryImpl cannot represent fractional seconds</a>. */
  @Test void testTranslateLiteralForFractionalSecondInterval() {
    final RexLiteral literal =
        REX_BUILDER.makeIntervalLiteral(
            new BigDecimal("1234.567"),
            new SqlIntervalQualifier(TimeUnit.SECOND, 2, null, 6,
                SqlParserPos.ZERO));

    final Expression expression =
        RexToLixTranslator.translateLiteral(literal, literal.getType(),
            TYPE_FACTORY, RexImpTable.NullAs.NOT_POSSIBLE);

    assertThat(expression.getType().getTypeName(), is(BigDecimal.class.getTypeName()));
    assertThat(Expressions.toString(expression), containsString("1234.567"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6752">[CALCITE-6752]
   * JavaTypeFactoryImpl cannot represent fractional seconds</a>. */
  @Test void testTranslateLiteralForMinuteIntervalStillUsesLong() {
    final RexLiteral literal =
        REX_BUILDER.makeIntervalLiteral(
            new BigDecimal("60000"),
            new SqlIntervalQualifier(TimeUnit.MINUTE, null, SqlParserPos.ZERO));

    final Expression expression =
        RexToLixTranslator.translateLiteral(literal, literal.getType(),
            TYPE_FACTORY, RexImpTable.NullAs.NOT_POSSIBLE);

    assertThat(expression.getType().getTypeName(), is(long.class.getTypeName()));
  }
}
