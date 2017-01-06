/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.tools.utils.MessageTracer;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * This class is for message handling operations of a queue. Handling
 * Message caching, buffering, keep trace of messages etc
 */
public class MessageHandler {


    private static Log log = LogFactory.getLog(MessageHandler.class);

    /**
     * Reference to MessageStore. This is the persistent storage for messages
     */
    private MessageStore messageStore;

    /**
     * Manager for message delivery to subscriptions.
     */
    private SlotDeliveryWorkerManager messageDeliveryManager;

    /**
     * Name of the queue to handle
     */
    private String queueName;

    /**
     * Maximum number to retries retrieve metadata list for a given storage
     * queue ( in the errors occur in message stores)
     */
    private static final int MAX_META_DATA_RETRIEVAL_COUNT = 5;

    /**
     * In-memory message list scheduled to be delivered. These messages will be flushed
     * to subscriber.Used Map instead of Set because of https://wso2.org/jira/browse/MB-1624
     */
    private ConcurrentMap<Long, DeliverableAndesMetadata> readButUndeliveredMessages = new
            ConcurrentSkipListMap<>();

    /**
     * ID of the last message read from store for the queue handled.
     * This will be used to read the next message chunk from store.
     */
    private long IdOfLastMessageRead;

    /**
     * Max number of messages to be read from DB in a single read
     */
    private int messageCountToRead;

    /**
     * In case of a purge, we must store the timestamp when the purge was called.
     * This way we can identify messages received before that timestamp that fail and ignore them.
     */
    private long lastPurgedTimestamp;

    /**
     * Max number of messages to keep in buffer
     */
    private Integer maxNumberOfReadButUndeliveredMessages;

    public MessageHandler(String queueName) {
        this.queueName = queueName;
        this.maxNumberOfReadButUndeliveredMessages = AndesConfigurationManager.
                readValue(AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_MAX_READ_BUT_UNDELIVERED_MESSAGES);
        this.messageCountToRead = AndesConfigurationManager.
                readValue(AndesConfiguration.PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE);
        this.messageDeliveryManager = SlotDeliveryWorkerManager.getInstance();
        this.lastPurgedTimestamp = 0L;
        this.messageStore = AndesContext.getInstance().getMessageStore();
        this.IdOfLastMessageRead = 0;
    }

    /**
     * Start delivering messages for queue
     *
     * @param queue queue to deliver messages
     * @throws AndesException
     */
    public void startMessageDelivery(StorageQueue queue) throws AndesException {
        messageDeliveryManager.startMessageDeliveryForQueue(queue);
    }

    /**
     * Stop delivering messages for queue
     *
     * @param queue queue to stop message delivery
     * @throws AndesException
     */
    public void stopMessageDelivery(StorageQueue queue) throws AndesException {
        messageDeliveryManager.stopDeliveryForQueue(queue);
    }

    /***
     * @return Last purged timestamp of queue.
     */
    public Long getLastPurgedTimestamp() {
        return lastPurgedTimestamp;
    }

    /**
     * Read messages from persistent storage for delivery
     * @return number of messages actually read from store
     * @throws AndesException on DB issue or issue when reading messages
     */
    public int bufferMessages() throws AndesException {

        long startMessageID = IdOfLastMessageRead + 1;

        List<DeliverableAndesMetadata> messagesReadFromStore = readMessagesFromMessageStore(startMessageID,
                messageCountToRead, queueName);

        if(!messagesReadFromStore.isEmpty()) {
            DeliverableAndesMetadata lastMessage = null;
            for (DeliverableAndesMetadata message : messagesReadFromStore) {
                bufferMessage(message);
                lastMessage = message;
            }
            if (lastMessage != null) {
                IdOfLastMessageRead = lastMessage.getMessageID();
            }
        }
        return messagesReadFromStore.size();
    }

    /**
     * Read messages from persistent storage for delivery
     *
     * @param startMessageID ID of message to start reading from
     * @param numOfMessagesToLoad max number of messages to read
     * @param storageQueueName queue name whose messages to be load to memory
     * @throws AndesException on DB issue or issue when reading messages
     */
    private List<DeliverableAndesMetadata> readMessagesFromMessageStore(long startMessageID, int numOfMessagesToLoad,
                                                                        String storageQueueName) throws AndesException {
        List<DeliverableAndesMetadata> messagesRead;
        int numberOfRetries = 0;
        try {

            //Read messages in the slot
            messagesRead = messageStore.getMetadataList(
                    storageQueueName,
                    startMessageID,
                    numOfMessagesToLoad);

            if (log.isDebugEnabled()) {
                StringBuilder messageIDString = new StringBuilder();
                for (DeliverableAndesMetadata metadata : messagesRead) {
                    messageIDString.append(metadata.getMessageID()).append(" , ");
                }
                log.debug("Messages Read: " + messageIDString);
            }

        } catch (AndesException aex) {

            numberOfRetries = numberOfRetries + 1;

            if (numberOfRetries <= MAX_META_DATA_RETRIEVAL_COUNT) {

                String errorMsg = String.format("error occurred retrieving metadata"
                        + " list for range :" + " %s, retry count = %d", "["
                        + startMessageID + ", " + numOfMessagesToLoad
                        + "]", numberOfRetries);

                log.error(errorMsg, aex);
                messagesRead = messageStore.getMetadataList(
                        storageQueueName,
                        startMessageID,
                        numOfMessagesToLoad);
            } else {
                String errorMsg = String.format("error occurred retrieving metadata list for range"
                        + ": %s, in final attempt = %d. " + "messages in this range is not read" ,
                        "[" + startMessageID + ", " + numOfMessagesToLoad
                                + "]", numberOfRetries);

                throw new AndesException(errorMsg, aex);
            }

        }

        if (log.isDebugEnabled()) {
            log.debug("Number of messages read from DB in range " + "[" + startMessageID + ", " + numOfMessagesToLoad
                    + "]"+ " is " + messagesRead.size() + " storage queue= " + storageQueueName);
        }

        return messagesRead;
    }

    /**
     * Get buffered messages
     *
     * @return Collection with DeliverableAndesMetadata
     */
    public Collection<DeliverableAndesMetadata> getReadButUndeliveredMessages() {
        return readButUndeliveredMessages.values();
    }

    /**
     * Buffer messages to be delivered
     *
     * @param message message metadata to buffer
     */
    public void bufferMessage(DeliverableAndesMetadata message) {
        readButUndeliveredMessages.putIfAbsent(message.getMessageID(), message);
        message.markAsBuffered();
        MessageTracer.trace(message, MessageTracer.METADATA_BUFFERED_FOR_DELIVERY);
    }


    /**
     * Returns boolean variable saying whether this destination has room or not
     *
     * @return whether this destination has room or not
     */
    public boolean messageBufferHasRoom() {
        boolean hasRoom = true;
        if (readButUndeliveredMessages.size() >= maxNumberOfReadButUndeliveredMessages) {
            hasRoom = false;
        }
        return hasRoom;
    }

    /***
     * Clear the read-but-undelivered collection of messages of the given queue from memory
     *
     * @return Number of messages that was in the read-but-undelivered buffer
     */
    public int clearReadButUndeliveredMessages() {
        lastPurgedTimestamp = System.currentTimeMillis();
        int messageCount = readButUndeliveredMessages.size();
        readButUndeliveredMessages.clear();
        return messageCount;
    }

    /**
     * Delete all messages of the specified queue
     *
     * @return how many messages were removed from queue
     * @throws AndesException
     */
    public int purgeMessagesOfQueue() throws AndesException {
        try {
            //clear all messages read to memory and return the slots
            clearReadButUndeliveredMessages();

            int deletedMessageCount;
            if (!(DLCQueueUtils.isDeadLetterQueue(queueName))) {
                // delete all messages for the queue
                deletedMessageCount = messageStore.deleteAllMessageMetadata(queueName);
            } else {
                //delete all the messages in dlc
                deletedMessageCount = messageStore.clearDLCQueue(queueName);
            }
            return deletedMessageCount;

        } catch (AndesException e) {
            // This will be a store-specific error.
            throw new AndesException("Error occurred when purging queue from store : " + queueName, e);
        }
    }

    /**
     * Get message count for queue. This will query the persistent store.
     *
     * @return number of messages remaining in persistent store addressed to queue
     * @throws AndesException
     */
    public long getMessageCountForQueue() throws AndesException {
        long messageCount;
        if (!DLCQueueUtils.isDeadLetterQueue(queueName)) {
            messageCount = messageStore.getMessageCountForQueue(queueName);
        } else {
            messageCount = messageStore.getMessageCountForDLCQueue(queueName);
        }
        return messageCount;
    }

    /**
     * Dump all message status of the slots read to file given
     *
     * @param fileToWrite      file to write
     * @param storageQueueName name of storage queue
     * @throws AndesException
     */
    public void dumpAllSlotInformationToFile(File fileToWrite, String storageQueueName) throws AndesException {
        //TODO: do we want this now?
    }

}
