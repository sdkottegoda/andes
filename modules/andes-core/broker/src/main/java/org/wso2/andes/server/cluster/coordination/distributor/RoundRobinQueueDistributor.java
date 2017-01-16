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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoundRobinQueueDistributor implements QueueDistributeStrategy {

    private AndesContextStore contextStore;

    public RoundRobinQueueDistributor(AndesContextStore contextStore) {
        this.contextStore = contextStore;
    }

    @Override
    public NodeInfo getMasterNode(String queueName) throws AndesException {
        //TODO: should we consider num of subscriptions as a load factor?
        List<NodeInfo> nodes = getAllNodes();
        for (NodeInfo node : nodes) {
            node.setNumOfAssignedQueues(getAssignedQueues(node.getNodeID()).size());
        }
        sortNodesByQueueAssignment(nodes);
        return nodes.get(0);
    }

    private List<NodeInfo> getAllNodes() throws AndesException {
        List<NodeInfo> nodesList = new ArrayList<>(5);
        Map<String, String> nodeData;
        nodeData = contextStore.getAllStoredNodeData();
        for (Map.Entry<String, String> nodeEntry : nodeData.entrySet()) {
            String nodeID = nodeEntry.getKey();
            String[] nodeDetail = nodeEntry.getValue().split(":");
            String hostName = nodeDetail[0];
            String jmsPort = nodeDetail[1];
            nodesList.add(new NodeInfo(nodeID, hostName, jmsPort));
        }

        return nodesList;
    }

    private List<String> getAssignedQueues(String nodeID) throws AndesException {
        return contextStore.getAllQueuesOwnedByNode(nodeID);
    }

    private void sortNodesByQueueAssignment(List<NodeInfo> nodes) throws AndesException {
        Collections.sort(nodes,new Comparator<NodeInfo>(){
            @Override
            public int compare(final NodeInfo lhs,NodeInfo rhs) {
                return lhs.getNumOfAssignedQueues() - rhs.getNumOfAssignedQueues();
                /*if(lhs.getNumOfAssignedQueues() < rhs.getNumOfAssignedQueues()) {
                    return 1;
                } else if (lhs.getNumOfAssignedQueues() > rhs.getNumOfAssignedQueues()) {
                    return -1;
                } else {
                    return 0;
                }*/
            }
        });
    }
}
