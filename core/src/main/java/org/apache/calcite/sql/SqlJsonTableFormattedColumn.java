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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Formatted column specification for {@code JSON_TABLE} function.
 */
public class SqlJsonTableFormattedColumn extends SqlJsonTableColumn {

  final SqlDataTypeSpec type;
  final SqlNode encoding;
  final @Nullable SqlNode path;
  final @Nullable SqlNode wrapper;
  final SqlNodeList behaviors;

  public SqlJsonTableFormattedColumn(
      SqlParserPos pos,
      SqlIdentifier name,
      SqlDataTypeSpec type,
      SqlNode encoding,
      @Nullable SqlNode path,
      @Nullable SqlNode wrapper,
      SqlNodeList behaviors) {
    super(pos, Objects.requireNonNull(name, "name"), Type.FORMATTED);
    this.type = Objects.requireNonNull(type, "type");
    this.encoding = Objects.requireNonNull(encoding, "encoding");
    this.path = path;
    this.wrapper = wrapper;
    this.behaviors = Objects.requireNonNull(behaviors, "behaviors");
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    throw Util.needToImplement(this);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, type, encoding, path, wrapper,
        behaviors);
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    throw Util.needToImplement(this);
  }
}
