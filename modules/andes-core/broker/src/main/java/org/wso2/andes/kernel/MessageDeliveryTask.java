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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.task.Task;

/**
 * Handle message delivery {@link Task} implementation for a given queue
 */
final class MessageDeliveryTask extends Task {

    private static Log log = LogFactory.getLog(MessageDeliveryTask.class);

    /**
     * The storage queue handled by this task.
     */
    private StorageQueue storageQueue;

    /**
     * Reference to {@link MessageFlusher} to deliver messages
     */
    private MessageFlusher messageFlusher;


    MessageDeliveryTask(StorageQueue storageQueue,
                        MessageFlusher messageFlusher) {

        this.storageQueue = storageQueue;
        this.messageFlusher = messageFlusher;
    }

    /**
     * Slot delivery task
     * {@inheritDoc}
     */
    @Override
    public TaskHint call() throws Exception {

        TaskHint taskHint = TaskHint.ACTIVE;

        String storageQueueName = storageQueue.getName();

        if (storageQueue.checkForReadMessageBufferLimit()) {

            int messagesRead = storageQueue.loadMessagesForDelivery();
            int numberOfMessageSent = sendMessagesToSubscriptions(storageQueue);
            if (0 == messagesRead) {
                if (numberOfMessageSent == 0) {
                    taskHint = TaskHint.IDLE; // Didn't do productive work
                }
            }

        } else {
            //If there are messages to be sent in the message buffer in MessageFlusher send them
            if (log.isDebugEnabled()) {
                log.debug("The queue " + storageQueueName + " has no room to buffer messages. " +
                        "Thus flushing the messages to subscriptions");
            }
            sendMessagesToSubscriptions(storageQueue);
        }
        return taskHint;
    }


    /**
     * Flush messages of queue to bounded subscriptions. This will get
     * a copy of active subscribers at the moment and send messages to them
     *
     * @return how many messages sent
     * @throws AndesException
     */
    public int sendMessagesToSubscriptions(StorageQueue queue) throws AndesException {
        return messageFlusher.sendMessagesToSubscriptions(queue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onAdd() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onRemove() {
        onStopDelivery();
    }

    /**
     * unque id of the {@link Task}
     * @return name of storage queue handle by this {@link MessageDeliveryTask}
     */
    @Override
    public String getId() {
        return storageQueue.getName();
    }

    /**
     * Update slot states when delivery stop
     */
    private void onStopDelivery() {
        storageQueue.clearMessagesReadToBufferForDelivery();
    }
}
