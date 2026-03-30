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
package org.apache.calcite.sql.parser.parserextensiontesting;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Simple test example of a CREATE OR ALTER TABLE statement.
 */
public class ExtensionSqlCreateOrAlterTable extends ExtensionSqlCreateTable {
  /** Creates a SqlCreateOrAlterTable. */
  public ExtensionSqlCreateOrAlterTable(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList, SqlNode query) {
    super(pos, name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE OR ALTER");
    writer.keyword("TABLE");
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      forEachNameType((name, typeSpec) -> {
        writer.sep(",");
        name.unparse(writer, leftPrec, rightPrec);
        typeSpec.unparse(writer, leftPrec, rightPrec);
        if (Boolean.FALSE.equals(typeSpec.getNullable())) {
          writer.keyword("NOT NULL");
        }
      });
      writer.endList(frame);
    }
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }
  }
}
