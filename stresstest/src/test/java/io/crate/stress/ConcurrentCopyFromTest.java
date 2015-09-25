/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.stress;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.google.common.collect.ImmutableList;
import io.crate.concurrent.Threaded;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.Iterator;

@Seed("[AC011FCEA24C061E:607B453D716D3C60]")
@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 3, scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class ConcurrentCopyFromTest extends AbstractIntegrationStressTest {

    private Iterator<String> tableSources;

    @Override
    public void prepareFirst() throws Exception {
        tableSources = ImmutableList.of(
                ConcurrentCopyFromTest.class.getResource("/setup/data/shn_m.eventsummary_has_detecteduser_0_trunc.json.gz").getPath(),
                ConcurrentCopyFromTest.class.getResource("/setup/data/shn_m.eventsummary_has_detecteduser_1_trunc.json.gz").getPath(),
                //ConcurrentCopyFromTest.class.getResource("/setup/data/shn_m.eventsummary_has_detecteduser_2_trunc.json.gz").getPath(),
                ConcurrentCopyFromTest.class.getResource("/setup/data/shn_m.eventsummary_has_detecteduser_2_trunc.json.gz").getPath()
        ).iterator();

        /*execute("CREATE TABLE doc.EventSummary_has_DetectedUser\n" +
                "(\n" +
                "    DetectedUser_id long primary key,\n" +
                "    FromTime timestamp primary key,\n" +
                "    EventSummary_id long primary key,\n" +
                "    CSP_id int,\n" +
                "    TimeUpdated timestamp,\n" +
                "    Tenant_Id int,\n" +
                "    Count long,\n" +
                "    TotalBytes long,\n" +
                "    UploadedBytes long,\n" +
                "    DownloadedBytes long,\n" +
                "    DLPCount long,\n" +
                "    DLPBytes long,\n" +
                "    ToTime timestamp,\n" +
                "    UserOrIP int,\n" +
                "    Device_id int,\n" +
                "    ServiceBlocked int,\n" +
                "    Monitor int,\n" +
                "    LogProcessorTag_id int,\n" +
                "    custom1 String,\n" +
                "    custom2 String,\n" +
                "    custom3 String,\n" +
                "    custom4 String,\n" +
                "    custom5 String,\n" +
                "    Protocol string\n" +
                ") CLUSTERED BY (DetectedUser_id) INTO 4 SHARDS PARTITIONED BY (FromTime)\n" +
                "  WITH (column_policy = 'strict', number_of_replicas=0)");*/
        execute("CREATE TABLE shn_m.EventSummary_has_DetectedUser\n" +
                "(\n" +
                "    DetectedUser_id long primary key,\n" +
                "    FromTime timestamp primary key,\n" +
                "    EventSummary_id long primary key,\n" +
                "    CSP_id int,\n" +
                "    TimeUpdated timestamp,\n" +
                "    Tenant_Id int,\n" +
                "    Count long,\n" +
                "    TotalBytes long,\n" +
                "    UploadedBytes long,\n" +
                "    DownloadedBytes long,\n" +
                "    DLPCount long,\n" +
                "    DLPBytes long,\n" +
                "    ToTime timestamp,\n" +
                "    UserOrIP int,\n" +
                "    Device_id int,\n" +
                "    ServiceBlocked int,\n" +
                "    Monitor int,\n" +
                "    LogProcessorTag_id int,\n" +
                "    custom1 String,\n" +
                "    custom2 String,\n" +
                "    custom3 String,\n" +
                "    custom4 String,\n" +
                "    custom5 String,\n" +
                "    Protocol string\n" +
                ") CLUSTERED BY (DetectedUser_id) INTO 30 SHARDS PARTITIONED BY (FromTime)\n" +
                "  WITH (column_policy = 'strict', number_of_replicas=0)");
        /*execute("CREATE TABLE shn_w.EventSummary_has_DetectedUser\n" +
                "(\n" +
                "    DetectedUser_id long primary key,\n" +
                "    FromTime timestamp primary key,\n" +
                "    EventSummary_id long primary key,\n" +
                "    CSP_id int,\n" +
                "    TimeUpdated timestamp,\n" +
                "    Tenant_Id int,\n" +
                "    Count long,\n" +
                "    TotalBytes long,\n" +
                "    UploadedBytes long,\n" +
                "    DownloadedBytes long,\n" +
                "    DLPCount long,\n" +
                "    DLPBytes long,\n" +
                "    ToTime timestamp,\n" +
                "    UserOrIP int,\n" +
                "    Device_id int,\n" +
                "    ServiceBlocked int,\n" +
                "    Monitor int,\n" +
                "    LogProcessorTag_id int,\n" +
                "    custom1 String,\n" +
                "    custom2 String,\n" +
                "    custom3 String,\n" +
                "    custom4 String,\n" +
                "    custom5 String,\n" +
                "    Protocol string\n" +
                ") CLUSTERED BY (DetectedUser_id) INTO 9 SHARDS " + // PARTITIONED BY (FromTime)\n" +
                "  WITH (column_policy = 'strict', number_of_replicas=0)");*/
        ensureYellow();
    }

    @Override
    public void cleanUpLast() throws Exception {
        Thread.sleep(2000);
    }

    @TestLogging("org.elasticsearch.action.bulk:TRACE")
    @Threaded(count=3)
    @Test
    public void testConcurrentCopyFrom() throws Exception {
        String source;
        synchronized (this) {
            source = tableSources.next();
        }
        System.out.println("starting COPY FROM " + source);
        execute("COPY shn_m.EventSummary_has_DetectedUser FROM ? with (compression='gzip', bulk_size=1000)", new Object[]{
            source
        }, TimeValue.timeValueMinutes(4));

    }
}
