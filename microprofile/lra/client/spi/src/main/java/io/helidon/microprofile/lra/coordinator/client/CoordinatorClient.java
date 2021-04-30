/*
 * Copyright (c) 2021 Oracle and/or its affiliates.
 *
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
 *
 */

package io.helidon.microprofile.lra.coordinator.client;

import java.net.URI;

import javax.ws.rs.WebApplicationException;

import org.eclipse.microprofile.lra.annotation.LRAStatus;

public interface CoordinatorClient {

    String CONF_KEY_COORDINATOR_URL = "mp.lra.coordinator.url";
    
    URI start(URI parentLRA, String clientID, Long timeout) throws WebApplicationException;
    void cancel(URI lraId) throws WebApplicationException;
    void close(URI lraId) throws WebApplicationException;
    URI join(URI lraId, Long timeLimit, Participant participant) throws WebApplicationException;
    void leave(URI lraId, Participant participant) throws WebApplicationException;
    LRAStatus status(URI uri) throws WebApplicationException;
}
