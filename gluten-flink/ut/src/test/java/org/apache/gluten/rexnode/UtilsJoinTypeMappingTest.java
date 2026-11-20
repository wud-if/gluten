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

import io.github.zhztheplayer.velox4j.join.JoinType;

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct tests for {@link Utils#toVLJoinType(FlinkJoinType)}: one assertion per FlinkJoinType value
 * (the enum has exactly these six) plus the null boundary falling into the else branch.
 */
public class UtilsJoinTypeMappingTest {

  @Test
  public void innerMapsToVeloxInner() {
    assertThat(Utils.toVLJoinType(FlinkJoinType.INNER)).isEqualTo(JoinType.INNER);
  }

  @Test
  public void leftMapsToVeloxLeft() {
    assertThat(Utils.toVLJoinType(FlinkJoinType.LEFT)).isEqualTo(JoinType.LEFT);
  }

  @Test
  public void rightMapsToVeloxRight() {
    assertThat(Utils.toVLJoinType(FlinkJoinType.RIGHT)).isEqualTo(JoinType.RIGHT);
  }

  @Test
  public void fullMapsToVeloxFull() {
    assertThat(Utils.toVLJoinType(FlinkJoinType.FULL)).isEqualTo(JoinType.FULL);
  }

  @Test
  public void semiMapsToVeloxLeftSemiFilter() {
    assertThat(Utils.toVLJoinType(FlinkJoinType.SEMI)).isEqualTo(JoinType.LEFT_SEMI_FILTER);
  }

  @Test
  public void antiMapsToVeloxAnti() {
    assertThat(Utils.toVLJoinType(FlinkJoinType.ANTI)).isEqualTo(JoinType.ANTI);
  }

  @Test
  public void nullJoinTypeFailsLoudInElseBranch() {
    // FlinkJoinType has exactly the six values above; null is the only constructible
    // else-branch input. Assert the existing behavior as-is (not any unconstructible
    // "unknown enum" case).
    assertThatThrownBy(() -> Utils.toVLJoinType(null))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Not supported join type: null");
  }
}
