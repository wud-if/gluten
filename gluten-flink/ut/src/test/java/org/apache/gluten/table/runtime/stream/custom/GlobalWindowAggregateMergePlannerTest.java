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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Planner tests of the gluten global window aggregate merge phase ({@code toMergeAggregates}):
 * COUNT/SUM/MAX/MIN window TVF aggregates must translate through explainSql with the two-phase
 * local/global merge segment, while DISTINCT window TVF aggregates must fail loud during
 * translation (at the local node's type conversion, before the merge phase).
 */
class GlobalWindowAggregateMergePlannerTest {

  private static StreamTableEnvironment tEnv;

  @BeforeAll
  static void setup() {
    Velox4jEnvironment.initializeOnce();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    tEnv = StreamTableEnvironment.create(env, settings);
  }

  private static void createRowtimeSource(String table) {
    tEnv.executeSql("drop table if exists " + table);
    tEnv.executeSql(
        "create table "
            + table
            + " (id int, v int, rowtime timestamp(3), "
            + "watermark for rowtime as rowtime - interval '1' second) with ("
            + "'connector' = 'datagen')");
  }

  @Test
  void testCountSumMaxMinMergeTranslates() {
    createRowtimeSource("gwaMergeSrc");
    String sql =
        "select id, count(*) as cnt, sum(v) as sv, max(v) as mx, min(v) as mn, "
            + "window_start, window_end "
            + "from table(tumble(table gwaMergeSrc, descriptor(rowtime), "
            + "interval '15' minute)) "
            + "group by id, window_start, window_end";
    // explainSql drives the full gluten translation including
    // StreamExecGlobalWindowAggregate#toMergeAggregates; a COUNT/SUM/MAX/MIN mix must
    // translate without throwing and surface the two-phase merge segment.
    String explain = tEnv.explainSql(sql);
    assertThat(explain).contains("LocalWindowAggregate");
    assertThat(explain).contains("GlobalWindowAggregate");
    // Merge segment shape: the global aggregate merges the local partials
    // (COUNT/SUM/MAX/MIN over the partial acc columns) instead of recomputing.
    assertThat(explain).contains("COUNT(count1$0) AS cnt");
    assertThat(explain).contains("SUM(sum$1) AS sv");
    assertThat(explain).contains("MAX(max$2) AS mx");
    assertThat(explain).contains("MIN(min$3) AS mn");
  }

  @Test
  void testDistinctWindowAggregateIsRejected() {
    createRowtimeSource("gwaDistinctSrc");
    String sql =
        "select id, count(distinct v) as dv, window_start, window_end "
            + "from table(tumble(table gwaDistinctSrc, descriptor(rowtime), "
            + "interval '15' minute)) "
            + "group by id, window_start, window_end";
    // Empirical behavior (explainSql drives the full gluten translation): a DISTINCT
    // window TVF aggregate still gets the local/global two-phase split, and the local
    // node fails loud first -- its output schema carries the DISTINCT accumulator's
    // MapView RAW column, which LogicalTypeConverter does not support
    // (StreamExecLocalWindowAggregate.java:174 -> LogicalTypeConverter.toVLType:136).
    // The global node's merge-phase distinct check
    // (StreamExecGlobalWindowAggregate.java:177) is shadowed by this earlier rejection.
    assertThatThrownBy(() -> tEnv.explainSql(sql))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Unsupported logical type: RAW('org.apache.flink.table.api.dataview.MapView'");
  }
}
