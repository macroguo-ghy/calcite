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
 * Nested columns specification for {@code JSON_TABLE} function.
 */
public class SqlJsonTableNestedColumns extends SqlJsonTableColumn {

  final SqlNode path;
  final SqlNodeList children;

  public SqlJsonTableNestedColumns(
      SqlParserPos pos,
      @Nullable SqlIdentifier name,
      SqlNode path,
      SqlNodeList children) {
    super(pos, name);
    this.path = Objects.requireNonNull(path, "path");
    this.children = Objects.requireNonNull(children, "children");
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("NESTED");
    writer.keyword("PATH");
    path.unparse(writer, leftPrec, rightPrec);
    if (name != null) {
      writer.keyword("AS");
      name.unparse(writer, leftPrec, rightPrec);
    }
    writer.newlineAndIndent();
    writer.keyword("COLUMNS");
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode child : children) {
      writer.sep(",");
      writer.newlineAndIndent();
      child.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return new SqlJsonTableNestedColumns(
        pos, Objects.requireNonNull(name, "name"), path, children);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, path, children);
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    RelDataTypeFactory.Builder builder = validator.getTypeFactory().builder();
    for (SqlNode node : children) {
      assert node instanceof SqlJsonTableColumn;
      SqlJsonTableColumn column = (SqlJsonTableColumn) node;
      RelDataType childType = column.deriveType(validator);
      assert childType.isStruct();
      childType.getFieldList().forEach(f -> builder.add(f.getName(), f.getType()));
    }
    return builder.build();
  }
}
