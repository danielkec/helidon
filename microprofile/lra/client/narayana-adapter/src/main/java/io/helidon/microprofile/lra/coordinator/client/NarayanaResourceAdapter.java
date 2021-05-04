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

package io.helidon.microprofile.lra.coordinator.client;

import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ResourceInfo;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_ENDED_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

@ApplicationScoped
public class NarayanaResourceAdapter implements CoordinatorResourceAdapter {

    private static final Logger LOGGER = Logger.getLogger(NarayanaResourceAdapter.class.getName());

    @Override
    public void handleJaxrsBefore(final ContainerRequestContext requestContext, final ResourceInfo resourceInfo) {
        // Narayana sends coordinator url as part of lraId with LRA_HTTP_ENDED_CONTEXT_HEADER
        // and parentLRA in lra header .../0_ffff7f000001_a76d_608fb07d_183a?ParentLRA=http%3A%2F%2...
        cleanupLraId(LRA_HTTP_CONTEXT_HEADER, requestContext);
        cleanupLraId(LRA_HTTP_ENDED_CONTEXT_HEADER, requestContext);
        cleanupLraId(LRA_HTTP_RECOVERY_HEADER, requestContext);
        cleanupLraId(LRA_HTTP_PARENT_CONTEXT_HEADER, requestContext);
    }

    @Override
    public void handleJaxrsAfter(ContainerRequestContext requestContext,
                                 ContainerResponseContext responseContext,
                                 ResourceInfo resourceInfo) {

    }

    private static void cleanupLraId(String headerKey, ContainerRequestContext reqCtx) {
        List<String> headers = Optional.ofNullable(reqCtx.getHeaders().get(headerKey)).orElse(List.of());
        if(headers.isEmpty()){
            return;
        }
        if (headers.size() > 1) {
            LOGGER.log(Level.SEVERE, "Ambiguous LRA header {0}}: {1}", new Object[] {
                    headerKey, String.join(", ", reqCtx.getHeaders().get(headerKey))
            });
        }
        String lraId = headers.get(0);
        if (lraId != null && lraId.contains("/lra-coordinator/")) {

            Matcher m = NarayanaLRAId.LRA_ID_PATTERN.matcher(lraId);
            if (!m.matches()) {
                //Unexpected header format from Narayana
                throw new RuntimeException("Error when parsing Narayana header " + headerKey + ": " + lraId);
            }
            reqCtx.getHeaders().putSingle(headerKey, m.group(1));
        }
    }
}
