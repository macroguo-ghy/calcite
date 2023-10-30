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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Regular column specification for {@code JSON_TABLE} function.
 */
public class SqlJsonTableRegularColumn extends SqlJsonTableColumn {

  final SqlDataTypeSpec type;
  final @Nullable SqlNode path;
  final SqlNodeList behaviors;

  public SqlJsonTableRegularColumn(
      SqlParserPos pos,
      SqlIdentifier name,
      SqlDataTypeSpec type,
      @Nullable SqlNode path,
      SqlNodeList behaviors) {
    super(pos, Objects.requireNonNull(name, "name"), Type.REGULAR);
    this.type = Objects.requireNonNull(type, "type");
    this.path = path;
    this.behaviors = Objects.requireNonNull(behaviors, "behaviors");
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    Objects.requireNonNull(name, "name").unparse(writer, leftPrec, rightPrec);
    type.unparse(writer, leftPrec, rightPrec);
    if (path != null) {
      writer.keyword("PATH");
      path.unparse(writer, leftPrec, rightPrec);
    }
    for (SqlNode behavior : behaviors) {
      behavior.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return new SqlJsonTableRegularColumn(
        pos, Objects.requireNonNull(name, "name"), type, path, behaviors);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, type, path, behaviors);
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    RelDataType relDataType = type.deriveType(validator);
    RelDataTypeFactory.Builder builder = validator.getTypeFactory().builder();
    builder.add(Objects.requireNonNull(name, "name").getSimple(), relDataType);
    derivedType = builder.build();
    return derivedType;
  }
}
