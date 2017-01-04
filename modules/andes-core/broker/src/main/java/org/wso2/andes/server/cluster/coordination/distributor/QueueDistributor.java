/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.server.cluster.coordination.distributor;


import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;

public class QueueDistributor {

    private AndesContextStore contextStore;
    private QueueDistributeStrategy queueDistributeStategy;

    public QueueDistributor(AndesContextStore contextStore) {
        this.contextStore = contextStore;
        this.queueDistributeStategy = new RoundRobinQueueDistributor(contextStore);
    }

    public synchronized String getMasterNode(String queueName, String protocol) throws AndesException {
        String nodeToAssignQueue;
        //check if queue is already assigned
        nodeToAssignQueue = contextStore.getQueueOwningNode(queueName);
        //if not assigned decide to which node
        if(null == nodeToAssignQueue) {
            NodeInfo node = queueDistributeStategy.getMasterNode(queueName);
            //store assignment in persistent store
            contextStore.assignNodeForQueue(queueName, node.getNodeID());
            nodeToAssignQueue = node.getHostName() + ":" + node.getAmqpPort();
        }
        //return hostname:port(AMQP) of assigned node to connect
        return nodeToAssignQueue;
    }
}
