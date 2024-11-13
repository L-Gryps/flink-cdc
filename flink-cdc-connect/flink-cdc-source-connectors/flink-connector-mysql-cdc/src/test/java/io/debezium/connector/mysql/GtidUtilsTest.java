/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.mysql;

import io.debezium.connector.mysql.strategy.mysql.MySqlGtidSet;
import org.junit.jupiter.api.Test;

import static io.debezium.connector.mysql.GtidUtils.fixRestoredGtidSet;
import static io.debezium.connector.mysql.GtidUtils.mergeGtidSetInto;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link GtidUtils}. */
class GtidUtilsTest {

    private static final String UUID1 = "24bc7850-2c16-11e6-a073-0242ac110002";
    private static final String UUID2 = "7145bf69-d1ca-11e5-a588-0242ac110004";
    private static final String UUID3 = "7c1de3f2-3fd2-11e6-9cdc-42010af000bc";

    @Test
    void testFixingRestoredGtidSet() {
        MySqlGtidSet serverGtidSet = new MySqlGtidSet(UUID1 + ":1-100");
        MySqlGtidSet restoredGtidSet = new MySqlGtidSet(UUID1 + ":30-100");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo(UUID1 + ":1-100");

        serverGtidSet = new MySqlGtidSet(UUID1 + ":1-100");
        restoredGtidSet = new MySqlGtidSet(UUID1 + ":30-50");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo(UUID1 + ":1-50");

        serverGtidSet = new MySqlGtidSet(UUID1 + ":1-100:102-200,"+ UUID2 + ":20-200");
        restoredGtidSet = new MySqlGtidSet(UUID1 + ":106-150");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo(UUID1 + ":1-100:102-150,"+ UUID2 + ":20-200");

        serverGtidSet = new MySqlGtidSet(UUID1 + ":1-100:102-200,"+ UUID2 + ":20-200");
        restoredGtidSet = new MySqlGtidSet(UUID1 + ":106-150,"+ UUID3 + ":1-100");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo(UUID1 + ":1-100:102-150,"+ UUID2 + ":20-200,"+ UUID3 + ":1-100");

        serverGtidSet = new MySqlGtidSet(UUID1 + ":1-100:102-200,"+ UUID2 + ":20-200");
        restoredGtidSet = new MySqlGtidSet(UUID1 + ":106-150:152-200,"+ UUID3 + ":1-100");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo(UUID1 + ":1-100:102-200,"+ UUID2 + ":20-200,"+ UUID3 + ":1-100");
    }

    @Test
    void testMergingGtidSets() {
        MySqlGtidSet base = new MySqlGtidSet(UUID1 + ":1-100");
        MySqlGtidSet toMerge = new MySqlGtidSet(UUID1 + ":1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString()).isEqualTo(UUID1 + ":1-100");

        base = new MySqlGtidSet(UUID1 + ":1-100");
        toMerge = new MySqlGtidSet(UUID2 + ":1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString()).isEqualTo(UUID1 + ":1-100,"+ UUID2 + ":1-10");

        base = new MySqlGtidSet(UUID1 + ":1-100,"+ UUID3 + ":1-100");
        toMerge = new MySqlGtidSet(UUID1 + ":1-10,"+ UUID2 + ":1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString()).isEqualTo(UUID1 + ":1-100,"+ UUID2 + ":1-10,"+ UUID3 + ":1-100");
    }
}
