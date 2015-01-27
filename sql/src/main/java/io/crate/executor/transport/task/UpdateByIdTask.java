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

package io.crate.executor.transport.task;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.exceptions.Exceptions;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.ShardUpdateRequest;
import io.crate.executor.transport.ShardUpdateResponse;
import io.crate.executor.transport.TransportShardUpdateAction;
import io.crate.planner.node.dml.UpdateByIdNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.UUID;

public class UpdateByIdTask extends AsyncChainedTask {

    private final TransportShardUpdateAction transport;
    private final ActionListener<ShardUpdateResponse> listener;
    private final ShardUpdateRequest request;


    private static class UpdateResponseListener implements ActionListener<ShardUpdateResponse> {

        private final SettableFuture<TaskResult> future;
        private final ESLogger logger = Loggers.getLogger(getClass());

        private UpdateResponseListener(SettableFuture<TaskResult> future) {
            this.future = future;
        }

        @Override
        public void onResponse(ShardUpdateResponse updateResponse) {
            int location = updateResponse.locations().get(0);
            if (updateResponse.responses().get(location) != null) {
                future.set(TaskResult.ONE_ROW);
            } else {
                ShardUpdateResponse.Failure failure = updateResponse.failures().get(location);
                if (failure.versionConflict()) {
                    logger.debug("Updating document with id {} failed because of a version conflict", failure.id());
                } else {
                    logger.error("Updating document with id {} failed {}", failure.id(), failure.message());
                }
                future.set(TaskResult.ZERO);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(Exceptions.unwrap(e));
        }
    }

    public UpdateByIdTask(UUID jobId, TransportShardUpdateAction transport, UpdateByIdNode node) {
        super(jobId);
        this.transport = transport;

        this.request = buildUpdateRequest(node);
        listener = new UpdateResponseListener(result);
    }

    @Override
    public void start() {
        transport.execute(this.request, this.listener);
    }

    protected ShardUpdateRequest buildUpdateRequest(UpdateByIdNode node) {
        // TODO: group items by shard and set routing to requests
        ShardUpdateRequest request = new ShardUpdateRequest(node.index(),
                node.assignmentsColumns(), node.missingAssignmentsColumns());
        //request.routing(node.routing());
        for (UpdateByIdNode.Item item : node.items()) {
            request.add(0, item.id(), item.assignments(), item.version(), item.missingAssignments());
        }
        return request;
    }
}
