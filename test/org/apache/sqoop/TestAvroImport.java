/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.config.ConfigurationConstants;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Tests --as-avrodatafile.
 */
public class TestAvroImport extends com.cloudera.sqoop.TestAvroImport {

  public static final Log LOG = LogFactory
      .getLog(TestAvroImport.class.getName());

  protected Configuration getConf() {
    Configuration conf = super.getConf();
    conf.setBoolean(ConfigurationConstants.PROP_ENABLE_AVRO_LOGICAL_TYPE_DECIMAL, true);
    return conf;
  }

  /**
   * Helper method that runs an import using Avro with optional command line
   * arguments and checks that the created file matches the expectations.
   * <p/>
   * This can be used to test various extra options that are implemented for
   * the Avro input.
   *
   * @param extraArgs extra command line arguments to pass to Sqoop in addition
   *                  to those that {@link #getOutputArgv(boolean, String[])}
   *                  returns
   */
  protected void avroImportTestHelper(String[] extraArgs, String codec)
      throws IOException {
    GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());

    String[] types =
      {"BIT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "VARCHAR(6)",
        "VARBINARY(2)", "DECIMAL(3,2)"};
    String[] vals = {"true", "100", "200", "1.0", "2.0", "'s'", "'0102'", "'1.00'"};
    createTableWithColTypes(types, vals);

    runImport(getOutputArgv(true, extraArgs));

    Path outputFile = new Path(getTablePath(), "part-m-00000.avro");
    DataFileReader<GenericRecord> reader = read(outputFile);
    Schema schema = reader.getSchema();
    assertEquals(Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(types.length, fields.size());

    checkField(fields.get(0), "DATA_COL0", Type.BOOLEAN);
    checkField(fields.get(1), "DATA_COL1", Type.INT);
    checkField(fields.get(2), "DATA_COL2", Type.LONG);
    checkField(fields.get(3), "DATA_COL3", Type.FLOAT);
    checkField(fields.get(4), "DATA_COL4", Type.DOUBLE);
    checkField(fields.get(5), "DATA_COL5", Type.STRING);
    checkField(fields.get(6), "DATA_COL6", Type.BYTES);
    checkField(fields.get(7), "DATA_COL7", Type.BYTES);

    GenericRecord record1 = reader.next();
    assertEquals("DATA_COL0", true, record1.get("DATA_COL0"));
    assertEquals("DATA_COL1", 100, record1.get("DATA_COL1"));
    assertEquals("DATA_COL2", 200L, record1.get("DATA_COL2"));
    assertEquals("DATA_COL3", 1.0f, record1.get("DATA_COL3"));
    assertEquals("DATA_COL4", 2.0, record1.get("DATA_COL4"));
    assertEquals("DATA_COL5", new Utf8("s"), record1.get("DATA_COL5"));
    Object object = record1.get("DATA_COL6");
    assertTrue(object instanceof ByteBuffer);
    ByteBuffer b = ((ByteBuffer) object);
    assertEquals((byte) 1, b.get(0));
    assertEquals((byte) 2, b.get(1));
    assertEquals("DATA_COL7", new BigDecimal("1.00"), record1.get("DATA_COL7"));

    if (codec != null) {
      assertEquals(codec, reader.getMetaString(DataFileConstants.CODEC));
    }

    checkSchemaFile(schema);
  }

  protected String getTableName() {
    return super.getTableName() + "_DECIMAL";
  }
}
