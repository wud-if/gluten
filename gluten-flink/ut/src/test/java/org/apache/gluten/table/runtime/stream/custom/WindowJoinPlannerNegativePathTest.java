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
package org.apache.gluten.table.runtime.stream.custom;

import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Fails-loud planner paths of the gluten window join: unsupported shapes must throw. */
class WindowJoinPlannerNegativePathTest {

  private static StreamTableEnvironment tEnv;

  @BeforeAll
  static void setup() {
    Velox4jEnvironment.initializeOnce();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    tEnv = StreamTableEnvironment.create(env, settings);
  }

  @Test
  void testProcTimeWindowJoinIsRejected() {
    tEnv.executeSql("drop table if exists wjProcL");
    tEnv.executeSql("drop table if exists wjProcR");
    tEnv.executeSql(
        "create table wjProcL (a int, b varchar, proctime as PROCTIME()) with ("
            + "'connector' = 'datagen')");
    tEnv.executeSql(
        "create table wjProcR (a int, b varchar, proctime as PROCTIME()) with ("
            + "'connector' = 'datagen')");
    String sql =
        "select L.a, R.a from "
            + "(select * from table(tumble(table wjProcL, descriptor(proctime), "
            + "interval '15' minute))) L "
            + "join (select * from table(tumble(table wjProcR, descriptor(proctime), "
            + "interval '15' minute))) R "
            + "on L.window_start = R.window_start and L.window_end = R.window_end and L.a = R.a";
    assertThatThrownBy(() -> tEnv.explainSql(sql))
        .isInstanceOf(TableException.class)
        .hasMessageContaining("Processing time Window Join is not supported yet.");
  }

  @Test
  void testNullSafeEquiKeyWindowJoinIsRejected() {
    tEnv.executeSql("drop table if exists wjRowtimeL");
    tEnv.executeSql("drop table if exists wjRowtimeR");
    tEnv.executeSql(
        "create table wjRowtimeL (a int, b varchar, rowtime timestamp(3), "
            + "watermark for rowtime as rowtime - interval '1' second) with ("
            + "'connector' = 'datagen')");
    tEnv.executeSql(
        "create table wjRowtimeR (a int, b varchar, rowtime timestamp(3), "
            + "watermark for rowtime as rowtime - interval '1' second) with ("
            + "'connector' = 'datagen')");
    String sql =
        "select L.a, R.a from "
            + "(select * from table(tumble(table wjRowtimeL, descriptor(rowtime), "
            + "interval '15' minute))) L "
            + "join (select * from table(tumble(table wjRowtimeR, descriptor(rowtime), "
            + "interval '15' minute))) R "
            + "on L.window_start = R.window_start and L.window_end = R.window_end "
            + "and L.a is not distinct from R.a";
    assertThatThrownBy(() -> tEnv.explainSql(sql))
        .isInstanceOf(TableException.class)
        .hasMessageContaining("WindowJoin does not support null-safe equi keys yet");
  }
}
