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
import org.wso2.andes.task.TaskExceptionHandler;

/**
 * Exception handler implementation for tasks
 */
final class DeliveryTaskExceptionHandler implements TaskExceptionHandler {

    private static Log log = LogFactory.getLog(DeliveryTaskExceptionHandler.class);

    @Override
    public void handleException(Throwable throwable, String taskId) {
        log.error("Error occurred while processing task. Task id " + taskId, throwable);
    }
}
