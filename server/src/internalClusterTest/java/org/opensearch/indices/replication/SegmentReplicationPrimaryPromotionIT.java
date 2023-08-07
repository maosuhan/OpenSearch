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
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class SegmentReplicationPrimaryPromotionIT extends SegmentReplicationBaseIT {

    @Override
    public boolean addMockInternalEngine() {
        return false;
    }

    public void testPromoteReplicaToPrimary() throws Exception {
        String[] dataNodeNames = getClusterState().nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).toArray(String[]::new);
        assertEquals(2, dataNodeNames.length);
        final String indexName = INDEX_NAME;
        createIndexInternal(indexName);
        ensureGreen();

        final int numOfDocs = 3;
        List<ActionFuture<IndexResponse>> responses = new ArrayList<>();
        for (int i = 0; i < numOfDocs; i++) {
            responses.add(client().index(new IndexRequest(indexName).source("long_fields", 1)));
        }
        assertBusy(() -> {
                responses.forEach(ActionFuture::actionGet);
                refresh(indexName);
            }
        );
        waitForSearchableDocs(numOfDocs, dataNodeNames);

        // pick up a data node that contains a random primary shard
        ClusterState state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
        final ShardRouting primaryShard = state.routingTable().index(indexName).shard(0).primaryShard();
        final ShardRouting replicaShard = state.routingTable().index(indexName).shard(0).replicaShards().get(0);
        final DiscoveryNode primaryNode = state.nodes().resolveNode(primaryShard.currentNodeId());
        final DiscoveryNode replicaNode = state.nodes().resolveNode(replicaShard.currentNodeId());

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode.getName()));
        ensureYellowAndNoInitializingShards(indexName);
        final DiscoveryNode newPrimaryNode = replicaNode;

        state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
        ShardRouting newPrimaryShardRouting = state.routingTable().index(indexName).shards().get(0).primaryShard();
        assertEquals(newPrimaryNode.getId(), newPrimaryShardRouting.currentNodeId());
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
    }

    private void createIndexInternal(String index) {
        assertAcked(client().admin()
            .indices()
            .prepareCreate(index)
            .setMapping("{\"_doc\":{\"properties\":{\"long_field\":{\"type\":\"long\"}}}}")
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            ));
    }
}
