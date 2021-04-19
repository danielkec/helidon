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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.UriBuilder;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

class NoAnnotationHandler implements AnnotationHandler {
    @Override
    public void handleJaxrsBefore(ContainerRequestContext requestContext,
                                  ResourceInfo resourceInfo) {
        // not LRA method at all, clear lra header
        String lraFromHeader = requestContext.getHeaders().getFirst(LRA_HTTP_CONTEXT_HEADER);
        requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);

        var existingLraId = LRAThreadContext.get().lra();

        if (existingLraId.isPresent()) {
            // Propagate thread bound lraId for after handler
            existingLraId.ifPresent(uri -> requestContext.setProperty("lra.id", existingLraId.get().toASCIIString()));
        } else if (lraFromHeader != null) {
            // Save lraId from header to thread local for possible clients
            LRAThreadContext.get().lra(UriBuilder.fromPath(lraFromHeader).build());
        }
    }

    @Override
    public void handleJaxrsAfter(ContainerRequestContext requestContext,
                                 ContainerResponseContext responseContext,
                                 ResourceInfo resourceInfo) {

    }
}
