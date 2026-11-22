/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.rexnode;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class DataPathMillisProbe
    extends org.apache.gluten.table.runtime.stream.common.GlutenStreamingTestBase {

  @Test
  void probe() {
    createSimpleBoundedValuesTable(
        "tblP",
        "b TIMESTAMP(3)",
        Arrays.asList(Row.of(java.time.LocalDateTime.parse("2024-01-15T03:04:05"))));
    String q1 = "select CAST(b AS BIGINT), DATE_FORMAT(b, 'yyyy-MM-dd HH:mm:ss') from tblP";
    for (String s : runQuery(q1)) {
      System.out.println("### PROBE " + s);
    }
    try {
      for (String s : runQuery("select to_timestamp_ltz(1705287845, 0)")) {
        System.out.println("### PROBE-LTZ-ONLY " + s);
      }
    } catch (Exception e) {
      System.out.println("### PROBE-LTZ-ONLY FAILED: " + e.getMessage().split("\n")[0]);
    }
    try {
      for (String s : runQuery("select DATE_FORMAT(b, 'z') from tblP")) {
        System.out.println("### PROBE-ZONE " + s);
      }
    } catch (Exception e) {
      System.out.println("### PROBE-ZONE FAILED: " + e.getMessage().split("\n")[0]);
    }
  }
}
