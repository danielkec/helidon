/*
 * Copyright (c) 2021 Oracle and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.helidon.microprofile.lra;

import java.net.URI;
import java.util.Optional;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ResourceInfo;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

class LeaveAnnotationHandler implements AnnotationHandler {

    private final CoordinatorClient coordinatorClient;
    private final ParticipantService participantService;

    public LeaveAnnotationHandler(CoordinatorClient coordinatorClient, final ParticipantService participantService) {
        this.coordinatorClient = coordinatorClient;
        this.participantService = participantService;
    }

    @Override
    public void handleJaxrsBefore(ContainerRequestContext requestContext, ResourceInfo resourceInfo) {
        Optional<URI> existingLraId = LRAThreadContext.get().lra();

        if (existingLraId.isPresent()) {
            var lraId = existingLraId.get();
            var baseUri = requestContext.getUriInfo().getBaseUri();
            var participant = participantService.participant(baseUri, resourceInfo.getResourceClass());
            coordinatorClient.leave(lraId, participant);
            requestContext.getHeaders().add(LRA_HTTP_CONTEXT_HEADER, lraId.toASCIIString());
            requestContext.setProperty("lra.id", lraId);
        }

    }

    @Override
    public void handleJaxrsAfter(final ContainerRequestContext requestContext,
                                 ContainerResponseContext responseContext,
                                 ResourceInfo resourceInfo) {

    }
}
