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


public class NodeInfo {

    private String nodeID;

    private String hostName;

    private String amqpPort;

    private int numOfAssignedQueues;

    public NodeInfo(String nodeID, String hostName, String amqpPort) {
        this.nodeID = nodeID;
        this.hostName = hostName;
        this.amqpPort = amqpPort;
        this.numOfAssignedQueues = 0;
    }

    public NodeInfo(String nodeID, String nodeInformation) {
        this.nodeID = nodeID;
        String[] hostPort = nodeInformation.split(":");
        this.hostName = hostPort[0];
        this.amqpPort = hostPort[1];
        this.numOfAssignedQueues = 0;
    }

    public String getNodeID() {
        return nodeID;
    }

    public String getHostName() {
        return hostName;
    }

    public String getAmqpPort() {
        return amqpPort;
    }

    void setNumOfAssignedQueues(int numOfAssignedQueues) {
        this.numOfAssignedQueues = numOfAssignedQueues;
    }
    int getNumOfAssignedQueues() {
        return numOfAssignedQueues;
    }
}
