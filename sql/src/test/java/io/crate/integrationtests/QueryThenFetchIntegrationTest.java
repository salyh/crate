/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.action.sql.SQLActionException;
import io.crate.test.integration.CrateIntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class QueryThenFetchIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCrateSearchServiceSupportsOrderByOnFunctionWithBooleanReturnType() throws Exception {
        execute("create table t (name string) with (number_of_replicas = 0)");
        execute("insert into t (name) values ('Marvin'), ('Trillian')");
        execute("refresh table t");
        ensureGreen();

        execute("select * from t order by substr(name, 1, 1) = 'M'");
        assertThat(((String) response.rows()[0][0]), is("Trillian"));
        assertThat(((String) response.rows()[1][0]), is("Marvin"));
    }

    @Test
    public void testThatErrorsInSearchResponseCallbackAreNotSwallowed() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(is("d != java.lang.String"));

        execute("create table t (s string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (s) values ('foo')");
        execute("refresh table t");

        execute("select format('%d', s) from t");
    }

    @Test
    public void testTestWithTimestampThatIsInIntegerRange() throws Exception {
        execute("create table t (ts timestamp) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (ts) values (0)");
        execute("insert into t (ts) values (1425980155)");
        execute("refresh table t");

        execute("select extract(day from ts) from t order by 1");
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(17));
    }
}
