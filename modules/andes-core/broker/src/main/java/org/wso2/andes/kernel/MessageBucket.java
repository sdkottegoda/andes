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

package org.wso2.andes.kernel;


import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.concurrent.ConcurrentMap;

public class MessageBucket {

    private ConcurrentMap<Long, DeliverableAndesMetadata> container;
    private DeliverableAndesMetadata lastMessageRead;
    private int numberOfMessagesRead;
    private StorageQueue queue;
    private StringBuilder messageIDString;

    public MessageBucket(StorageQueue queue, ConcurrentMap<Long, DeliverableAndesMetadata> container) {
        this.queue = queue;
        this.container = container;
        this.numberOfMessagesRead = 0;
        this.messageIDString = new StringBuilder();
    }

    public void bufferMessage(DeliverableAndesMetadata message) {
        container.putIfAbsent(message.getMessageID(), message);
        postRead(message);
        lastMessageRead = message;
        numberOfMessagesRead = numberOfMessagesRead +1;
    }

    private void postRead(DeliverableAndesMetadata message) {
        messageIDString.append(message.messageID).append(" , ");
        message.markAsBuffered();
        MessageTracer.trace(message, MessageTracer.METADATA_BUFFERED_FOR_DELIVERY);
    }

    public DeliverableAndesMetadata lastMessageRead() {
        return lastMessageRead;
    }

    public String getQueueName() {
        return queue.getName();
    }

    public int numberOfMessagesRead() {
        return numberOfMessagesRead;
    }

    public int size() {
        return container.size();
    }

    public String messagesReadAsStringVal() {
        return messageIDString.toString();
    }
}
