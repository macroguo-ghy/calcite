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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonTableColumn;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The <code>JSON_TABLE</code> function.
 */
public class SqlJsonTableFunction
    extends SqlFunction implements SqlTableFunction {
  public SqlJsonTableFunction() {
    super("JSON_TABLE", SqlKind.OTHER_FUNCTION,
        ReturnTypes.CURSOR, null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);
  }

  @Override public String getAllowedSignatures(String opNameToUse) {
    return "JSON_TABLE(json_doc, path"
        + " COLUMNS(<JSON_TABLE_COLUMN_DEFINITION>"
        + " [, <JSON_TABLE_COLUMN_DEFINITION>])"
        + " [(ERROR | EMPTY) ON ERROR])";
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    for (int i = 0; i <= 1; i++) {
      writer.sep(",");
      call.operand(i).unparse(writer, leftPrec, rightPrec);
    }
    writer.newlineAndIndent();
    writer.keyword("COLUMNS");
    final SqlWriter.Frame columnsFrame = writer.startList("(", ")");
    List<SqlNode> columns = call.getOperandList().stream()
        .filter(o -> o instanceof SqlJsonTableColumn)
        .collect(Collectors.toList());
    int i = 2;
    for (SqlNode column : columns) {
      writer.sep(",");
      writer.newlineAndIndent();
      column.unparse(writer, leftPrec, rightPrec);
      i++;
    }
    writer.endList(columnsFrame);
    writer.newlineAndIndent();

    if (i < call.operandCount()) {
      call.operand(i).unparse(writer, leftPrec, rightPrec);
      writer.keyword("ON");
      writer.keyword("ERROR");
      writer.newlineAndIndent();
    }
    writer.endFunCall(frame);
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return opBinding -> {
      RelDataTypeFactory.Builder builder = opBinding.getTypeFactory().builder();
      for (int i = 2; i < opBinding.getOperandCount() - 1; i++) {
        RelDataType type = opBinding.getOperandType(i);
        assert type.isStruct();
        type.getFieldList().forEach(f -> builder.add(f.getName(), f.getType()));
      }
      return builder.build();
    };
  }
}
