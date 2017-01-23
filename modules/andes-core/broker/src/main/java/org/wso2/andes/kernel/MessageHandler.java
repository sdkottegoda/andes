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
import org.wso2.andes.store.cache.AndesMessageCache;
import org.wso2.andes.store.cache.MessageCacheFactory;
import org.wso2.andes.tools.utils.MessageTracer;

import java.io.File;
import java.util.Collection;
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
     * Queue to handle messages for
     */
    private StorageQueue queue;

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
     * Cache to keep message metadata
     */
    private AndesMessageCache metadataCache;

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

    private boolean isInMemoryModeActive;

    public MessageHandler(StorageQueue queue) {
        this.queue = queue;
        this.metadataCache = (new MessageCacheFactory()).create(queue);
        this.maxNumberOfReadButUndeliveredMessages = AndesConfigurationManager.
                readValue(AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_MAX_READ_BUT_UNDELIVERED_MESSAGES);
        this.messageCountToRead = AndesConfigurationManager.
                readValue(AndesConfiguration.PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE);
        this.isInMemoryModeActive = AndesConfigurationManager.
                readValue(AndesConfiguration.PERSISTENCE_IN_MEMORY_MODE_ACTIVE);
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

        MessageBucket messageBucket = new MessageBucket(queue, readButUndeliveredMessages);

        long startMessageID = IdOfLastMessageRead + 1;

        if(metadataCache.isOperational()) {
            if(log.isDebugEnabled()) {
                log.debug("Reading message metadata from cache. [count= " + messageCountToRead + ", queue= " + queue
                        .getName());
            }

            metadataCache.readMessagesFromCache(messageCountToRead, messageBucket);

            if(log.isDebugEnabled()) {
                log.debug("Read messages from cache for queue " + queue.getName()
                        + ":" + messageBucket.messagesReadAsStringVal());
            }

            if((!isInMemoryModeActive) && (messageBucket.numberOfMessagesRead() == 0)) {
                readMessagesFromMessageStore(startMessageID,
                        messageCountToRead, messageBucket);
                if(messageBucket.numberOfMessagesRead() > 0) {
                    metadataCache.disable();        //messages left in DB. Disabling cache
                }
            }

        } else {
            readMessagesFromMessageStore(startMessageID,
                    messageCountToRead, messageBucket);
        }
        if (messageBucket.lastMessageRead() != null) {
            IdOfLastMessageRead = messageBucket.lastMessageRead().getMessageID();
        }
        return messageBucket.numberOfMessagesRead();
    }

    /**
     * Read messages from persistent storage for delivery
     *
     * @param startMessageID      ID of message to start reading from
     * @param numOfMessagesToLoad max number of messages to read
     * @param messageBucket       container to fill messages
     * @throws AndesException on DB issue or issue when reading messages
     */
    private void readMessagesFromMessageStore(long startMessageID, int numOfMessagesToLoad,
                                              MessageBucket messageBucket) throws AndesException {
        int numberOfRetries = 0;
        try {

            //Read messages in the slot
            messageStore.getMetadataList(
                    messageBucket,
                    startMessageID,
                    numOfMessagesToLoad);

            if (log.isDebugEnabled()) {
                log.debug("Messages Read for queue " + queue.getName()
                        + " from DB: " + messageBucket.messagesReadAsStringVal());
            }

        } catch (AndesException aex) {

            numberOfRetries = numberOfRetries + 1;

            if (numberOfRetries <= MAX_META_DATA_RETRIEVAL_COUNT) {

                String errorMsg = String.format("error occurred retrieving metadata"
                        + " list for range :" + " %s, retry count = %d", "["
                        + startMessageID + ", " + numOfMessagesToLoad
                        + "]", numberOfRetries);

                log.error(errorMsg, aex);
                messageStore.getMetadataList(
                        messageBucket,
                        startMessageID,
                        numOfMessagesToLoad);
            } else {
                String errorMsg = String.format("error occurred retrieving metadata list for range"
                                + ": %s, in final attempt = %d. " + "messages in this range is not read",
                        "[" + startMessageID + ", " + numOfMessagesToLoad
                                + "]", numberOfRetries);

                throw new AndesException(errorMsg, aex);
            }

        }

        if (log.isDebugEnabled()) {
            log.debug("Number of messages read from DB in range " + "[" + startMessageID + ", " + numOfMessagesToLoad
                    + "]" + " is " + messageBucket.numberOfMessagesRead() + " storage queue= "
                    + messageBucket.getQueueName());
        }

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
        resetMessageReadingCurser();
        return messageCount;
    }

    private void resetMessageReadingCurser() {
        //metadataCache.disable();
        IdOfLastMessageRead = 0;
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
            if (!(DLCQueueUtils.isDeadLetterQueue(queue.getName()))) {
                // delete all messages for the queue
                deletedMessageCount = messageStore.deleteAllMessageMetadata(queue.getName());
            } else {
                //delete all the messages in dlc
                deletedMessageCount = messageStore.clearDLCQueue(queue.getName());
            }
            return deletedMessageCount;

        } catch (AndesException e) {
            // This will be a store-specific error.
            throw new AndesException("Error occurred when purging queue from store : " + queue, e);
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
        if (!DLCQueueUtils.isDeadLetterQueue(queue.getName())) {
            messageCount = messageStore.getMessageCountForQueue(queue.getName());
        } else {
            messageCount = messageStore.getMessageCountForDLCQueue(queue.getName());
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

    /**
     * Store incoming message to queue
     *
     * @param message message to store
     * @throws AndesException in case of an exception at message store
     */
    public void storeMessage(AndesMessage message) throws AndesException{
        metadataCache.addToCache(message);
    }

    /**
     * Get total number of messages came in for queue since broker start
     *
     * @return total number of incoming messages for the queue. -1 if queue
     * master node is not this node
     */
    public long getTotalReceivedMessageCount() throws AndesException {
        return messageStore.getTotalReceivedMessageCount(queue.getName());
    }

    /**
     * Get total number of messages acknowledged for queue since broker
     * start.
     *
     * @return total number of messages acknowledged for queue. -1 if queue
     * master node is not this node
     */
    public long getTotalAckedMessageCount() throws AndesException {
        return messageStore.getTotalAckedMessageCount(queue.getName());
    }
}
