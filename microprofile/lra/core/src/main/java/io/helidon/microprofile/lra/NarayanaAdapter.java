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

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ResourceInfo;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_ENDED_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

@ApplicationScoped
public class NarayanaAdapter implements CoordinatorFilteringAdapter{
    @Override
    public void handleJaxrsBefore(final ContainerRequestContext requestContext, final ResourceInfo resourceInfo) {
        // Narayana sends coordinator url as part of lraId with LRA_HTTP_ENDED_CONTEXT_HEADER
        cleanupLraId(LRA_HTTP_CONTEXT_HEADER, requestContext);
        cleanupLraId(LRA_HTTP_ENDED_CONTEXT_HEADER, requestContext);
        cleanupLraId(LRA_HTTP_RECOVERY_HEADER, requestContext);
    }

    @Override
    public void handleJaxrsAfter(ContainerRequestContext requestContext, 
                                 ContainerResponseContext responseContext, 
                                 ResourceInfo resourceInfo) {

    }
    
    private static void cleanupLraId(String headerKey, ContainerRequestContext reqCtx){
        String endedLraId = reqCtx.getHeaders().getFirst(headerKey);
        if (endedLraId != null && endedLraId.contains("/lra-coordinator/")) {
            reqCtx.getHeaders().putSingle(headerKey, endedLraId.split("/lra-coordinator/")[1]);
        }
    }
}
