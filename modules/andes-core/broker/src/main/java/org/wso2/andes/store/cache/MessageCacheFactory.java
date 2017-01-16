/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
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

package org.wso2.andes.store.cache;

import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.subscription.StorageQueue;

/**
 * Factory to create a {@link AndesMessageCache} based on the configurations in broker.xml 
 *
 */
public class MessageCacheFactory {


    /***
     * Create a {@link AndesMessageCache} with the configurations passed.
     * currently it will either returns a {@link ExtendedMessageCacheImpl} or
     * {@link AlwaysOnExtendedMessageCacheImpl} if broker is operate in-memory mode
     * @param queue storage queue whose messages should be stored in the cache. If this param is null, factory will
     *              create a generic cache to store any message.
     * @return a {@link AndesMessageCache}
     */
    public AndesMessageCache create(StorageQueue queue) {

        AndesMessageCache cache;

        boolean isInMemoryModeActive = AndesConfigurationManager.
                readValue(AndesConfiguration.PERSISTENCE_IN_MEMORY_MODE_ACTIVE);

        if(isInMemoryModeActive) {
            cache = new AlwaysOnExtendedMessageCacheImpl(queue);
        } else {
            cache = new ExtendedMessageCacheImpl(queue);
        }

        return cache;
    }

    /***
     * Create a {@link AndesMessageCache} with the configurations passed.
     * currently it will either returns a {@link GuavaBasedMessageCacheImpl} or
     * {@link DisabledMessageCacheImpl} if cacheSize is configured as '0' in
     * broker.xml. If broker operates in-memory mode it returns a {@link AlwaysOnMessageCacheImpl}
     * @return a {@link AndesMessageCache}
     */
    public AndesMessageCache create() {
        AndesMessageCache cache;

        boolean isInMemoryModeActive = AndesConfigurationManager.
                readValue(AndesConfiguration.PERSISTENCE_IN_MEMORY_MODE_ACTIVE);

        int cacheSizeInMegaBytes = AndesConfigurationManager.
                readValue(AndesConfiguration.PERSISTENCE_CACHE_SIZE);

        if (isInMemoryModeActive) {
            cache = new AlwaysOnMessageCacheImpl();
        } else {
            if (cacheSizeInMegaBytes <= 0) {
                cache = new DisabledMessageCacheImpl();
            } else {
                cache = new GuavaBasedMessageCacheImpl();
            }
        }

        return cache;
    }
    
}
