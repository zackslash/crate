/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class CrossJoinSingleNodeIntegrationTest extends SQLTransportIntegrationTest {

    private static int NUMBER_OF_SHARDS_PER_TABLE = 1;

    @Before
    public void createDataSet() throws Exception {
        execute(String.format("create table colors (name string) clustered into %d shards with (number_of_replicas=0)", NUMBER_OF_SHARDS_PER_TABLE));
        execute(String.format("create table sizes (name string) clustered into %d shards with (number_of_replicas=0)", NUMBER_OF_SHARDS_PER_TABLE));
        execute(String.format("create table genders (name string) clustered into %d shards with (number_of_replicas=0)", NUMBER_OF_SHARDS_PER_TABLE));
        execute(String.format("create table cities (name string) clustered into %d shards with (number_of_replicas=0)", NUMBER_OF_SHARDS_PER_TABLE));
        ensureYellow();

        execute("insert into colors (name) values (?)", new Object[][]{
                new Object[]{"red"},
                new Object[]{"blue"},
                new Object[]{"green"}
        });
        execute("insert into sizes (name) values (?)", new Object[][]{
                new Object[]{"small"},
                new Object[]{"large"},
        });
        execute("insert into genders (name) values (?)", new Object[][]{
                new Object[]{"female"},
                new Object[]{"male"},
        });
        execute("insert into cities (name) values (?)", new Object[][]{
                new Object[]{"Berlin"},
                new Object[]{"Dornbirn"},
        });
        execute("refresh table colors, sizes, genders, cities");

    }

    @Repeat(iterations = 50)
    @Test
    @TestLogging("io.crate.action.job:TRACE, io.crate.jobs:TRACE")
    public void testCrossJoinMultipleTables() throws Exception {
        /*
        execute("select colors.name, sizes.name, genders.name, cities.name " +
                "from colors, sizes, genders, cities");
        assertThat(response.rowCount(), is(24L));
        assertThat(printedTable(response.rows()), containsString("" +
                "blue| large| female| Berlin\n"));
        assertThat(printedTable(response.rows()), containsString("" +
                "red| small| male| Dornbirn\n"));
        */

        execute("select colors.name, sizes.name, genders.name " +
                "from colors, sizes, genders");
        assertThat(response.rowCount(), is(12L));
        assertThat(printedTable(response.rows()), containsString("" +
                "blue| large| female\n"));
        assertThat(printedTable(response.rows()), containsString("" +
                "red| small| male\n"));
    }

    @Repeat(iterations = 50)
    @Test
    @TestLogging("io.crate.action.job:TRACE, io.crate.jobs:TRACE")
    public void testJoinMultipleTablesWithOrderBy() throws Exception {
        execute("select colors.name, sizes.name, genders.name from colors, sizes, genders " +
                "order by sizes.name, colors.name, genders.name");
        assertThat(response.rowCount(), is(12L));
        assertThat(printedTable(response.rows()), is("" +
                "blue| large| female\n" +
                "blue| large| male\n" +
                "green| large| female\n" +
                "green| large| male\n" +
                "red| large| female\n" +
                "red| large| male\n" +
                "blue| small| female\n" +
                "blue| small| male\n" +
                "green| small| female\n" +
                "green| small| male\n" +
                "red| small| female\n" +
                "red| small| male\n"));
    }
}
