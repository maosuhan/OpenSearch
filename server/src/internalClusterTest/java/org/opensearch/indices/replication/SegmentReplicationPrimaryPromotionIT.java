/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.opensearch.indices.replication;

import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class SegmentReplicationPrimaryPromotionIT extends OpenSearchIntegTestCase {

    @Override
    public boolean addMockInternalEngine() {
        return false;
    }

    public void testPromoteReplicaToPrimary() throws Exception {
        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        createIndexInternal(indexName);
        ensureGreen();

        final int numOfDocs = 3;
        List<IndexRequest> requestList = new ArrayList<>();
        for (int i = 0; i < numOfDocs; i++) {
            requestList.add(new IndexRequest(indexName).source("long_fields", 1));
        }
        List<ActionFuture<IndexResponse>> responseList = new ArrayList<>();
        requestList.forEach(request -> responseList.add(client().index(request)));
        responseList.forEach(ActionFuture::actionGet);

        refresh(indexName);
        // primary and replica refresh is not consistent currently in segment replication mode
        Thread.sleep(1000L);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
        ensureGreen(indexName);

        // pick up a data node that contains a random primary shard
        ClusterState state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
        final int numShards = state.metadata().index(indexName).getNumberOfShards();
        final ShardRouting primaryShard = state.routingTable().index(indexName).shard(randomIntBetween(0, numShards - 1)).primaryShard();
        final DiscoveryNode randomNode = state.nodes().resolveNode(primaryShard.currentNodeId());

        // stop the data node of primary shard, all remaining shards are promoted to primaries
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(randomNode.getName()));
        ensureYellowAndNoInitializingShards(indexName);

        state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
        for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index(indexName)) {
            for (ShardRouting shardRouting : shardRoutingTable.activeShards()) {
                assertThat(shardRouting + " should be promoted as a primary", shardRouting.primary(), is(true));
            }
        }

        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
    }

    private void createIndexInternal(String index) {
        client().admin()
            .indices()
            .prepareCreate(index)
            .setMapping("{\"_doc\":{\"properties\":{\"long_field\":{\"type\":\"long\"}}}}")
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            )
            .get();
    }
}
