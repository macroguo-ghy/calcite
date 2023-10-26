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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Ordinality column specification for {@code JSON_TABLE} function.
 */
public class SqlJsonTableOrdinalityColumn extends SqlJsonTableColumn {

  public SqlJsonTableOrdinalityColumn(
      SqlParserPos pos, SqlIdentifier name) {
    super(pos, Objects.requireNonNull(name, "name"));
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    Objects.requireNonNull(name, "name").unparse(writer, leftPrec, rightPrec);
    writer.keyword("FOR");
    writer.keyword("ORDINALITY");
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return new SqlJsonTableOrdinalityColumn(
        pos, Objects.requireNonNull(name, "name"));
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name);
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    RelDataTypeFactory.Builder builder = validator.getTypeFactory().builder();
    builder.add(Objects.requireNonNull(name, "name").getSimple(), SqlTypeName.INTEGER);
    return builder.build();
  }
}
