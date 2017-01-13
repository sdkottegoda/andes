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

package org.wso2.andes.store.cache;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.MessageBucket;
import org.wso2.andes.kernel.subscription.StorageQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ExtendedMessageCacheImpl extends GuavaBasedMessageCacheImpl{

    private static final Logger log = Logger.getLogger(ExtendedMessageCacheImpl.class);

    private boolean isCacheInOperation;

    private StorageQueue storageQueue;

    private ConcurrentLinkedQueue<Long> messageIdsQueuedInOrder;

    public ExtendedMessageCacheImpl(StorageQueue storageQueue) {
        super();
        this.storageQueue = storageQueue;
        this.isCacheInOperation = true;
        this.messageIdsQueuedInOrder = new ConcurrentLinkedQueue<>();
    }

    public boolean isOperational() {
        return isCacheInOperation;
    }

    public void enable() {
        isCacheInOperation = true;
    }

    public void disable() {
        isCacheInOperation = false;
        clear();
    }

    public void addToCache(AndesMessage message) {
        //add to cache. If non-operational do nothing
        if(isCacheInOperation) {
            messageIdsQueuedInOrder.add(message.getMetadata().getMessageID());
            super.cache.put(message.getMetadata().getMessageID(), message);
        }
    }

    public AndesMessage getMessageFromCache(long messageId) {
        //if there is a cache miss, invalidate all and restore from DB
        //at the fist time, place a warning that switched to DB mode
        AndesMessage message = cache.getIfPresent(messageId);
        if(log.isDebugEnabled()) {
            log.debug("Retrieved message id " + messageId + " from cache");
        }
        if (null == message) {
            log.info("Subscribers are slower than publishers for queue " + storageQueue + ". Since cache will not be "
                    + "used");
            this.disable();
        } else {
            cache.invalidate(message);
        }
        return message;
    }

    private void clear() {
        cache.invalidateAll();
        //drain the queue
        Object[] messageIDs = messageIdsQueuedInOrder.toArray();
        for (Object messageID : messageIDs) {
            messageIdsQueuedInOrder.remove(messageID);
        }
    }

    public void readMessagesFromCache(int messageCountToRead, MessageBucket messageBucket) {
        int numOfMessagesReceived = 0;
        while (numOfMessagesReceived < messageCountToRead) {
            Long messageID = messageIdsQueuedInOrder.poll();
            if(null != messageID) {
                AndesMessage message = getMessageFromCache(messageID);
                if(null != message) {
                    DeliverableAndesMetadata deliverableMessage = new DeliverableAndesMetadata(message.getMetadata());
                    messageBucket.bufferMessage(deliverableMessage);
                    numOfMessagesReceived = numOfMessagesReceived + 1;
                } else {
                    break;  //cache miss. Subscriber is slow. Switching to DB.
                }
            } else {
                break;  //queue is empty. We have hit the bottom of the queue. Publisher is slower
            }
        }
    }



//    private class PersistingRemovalListener implements RemovalListener<K, V> {
//        @Override
//        public void onRemoval(RemovalNotification<K, V> notification) {
//            if (notification.getCause() != RemovalCause.COLLECTED) {
//                try {
//                    persistValue(notification.getKey(), notification.getValue());
//                } catch (IOException e) {
//                    LOGGER.error(String.format("Could not persist key-value: %s, %s",
//                            notification.getKey(), notification.getValue()), e);
//                }
//            }
//        }
//    }
}
