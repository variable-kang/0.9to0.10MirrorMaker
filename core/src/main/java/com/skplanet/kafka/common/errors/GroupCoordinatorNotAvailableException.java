/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.skplanet.kafka.common.errors;

/**
 * The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has
 * not yet been created.
 */
public class GroupCoordinatorNotAvailableException extends RetriableException {
    public static final GroupCoordinatorNotAvailableException INSTANCE = new GroupCoordinatorNotAvailableException();

    private static final long serialVersionUID = 1L;

    public GroupCoordinatorNotAvailableException() {
        super();
    }

    public GroupCoordinatorNotAvailableException(String message) {
        super(message);
    }

    public GroupCoordinatorNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public GroupCoordinatorNotAvailableException(Throwable cause) {
        super(cause);
    }

}