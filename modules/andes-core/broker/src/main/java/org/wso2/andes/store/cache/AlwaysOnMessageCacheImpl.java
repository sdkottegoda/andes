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

import com.google.common.cache.CacheBuilder;
import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

public class AlwaysOnMessageCacheImpl extends GuavaBasedMessageCacheImpl{

    private static final Logger log = Logger.getLogger(AlwaysOnMessageCacheImpl.class);

    /**
     * Cache values can be kept using weak references.
     */
    private static final String CACHE_VALUE_REF_TYPE_WEAK = "weak";

    /**
     * Flag indicating guava cache statistics should be printed on logs
     */
    private final boolean printStats;


    public AlwaysOnMessageCacheImpl() {

        super();

        int cacheConcurrency = AndesConfigurationManager
                .readValue(AndesConfiguration.PERSISTENCE_CACHE_CONCURRENCY_LEVEL);

        String valueRefType = AndesConfigurationManager
                .readValue(AndesConfiguration.PERSISTENCE_CACHE_VALUE_REFERENCE_TYPE);
        printStats = AndesConfigurationManager.readValue(AndesConfiguration.PERSISTENCE_CACHE_PRINT_STATS);

        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().concurrencyLevel(cacheConcurrency);

        if (printStats) {
            builder = builder.recordStats();
        }

        if (CACHE_VALUE_REF_TYPE_WEAK.equalsIgnoreCase(valueRefType)) {
            builder = builder.weakValues();
        }

        this.cache = builder.build();
    }


    public boolean isOperational() {
        return true;
    }

    public void disable() {
        throw new UnsupportedOperationException("AlwaysOnExtendedMessageCacheImpl cannot be disabled");
    }

    @Override
    public void enable() {
        //always enabled
    }
}
