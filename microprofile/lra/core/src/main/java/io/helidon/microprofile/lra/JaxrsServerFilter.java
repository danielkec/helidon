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
 */
package io.helidon.microprofile.lra;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.ws.rs.ConstrainedTo;
import javax.ws.rs.RuntimeType;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriBuilder;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

@ConstrainedTo(RuntimeType.SERVER)
public class JaxrsServerFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOGGER = Logger.getLogger(JaxrsServerFilter.class.getName());

    private static final ThreadLocal<URI> threadLocal = new ThreadLocal<>();

    @Context
    protected ResourceInfo resourceInfo;

    @Inject
    CoordinatorClient coordinatorClient;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        try {
            Method method = resourceInfo.getResourceMethod();

            // if lraId already exists save it for later
            Optional.ofNullable(requestContext.getHeaders().getFirst(LRA_HTTP_CONTEXT_HEADER))
                    .map(h -> UriBuilder.fromPath(requestContext.getHeaders().getFirst(LRA_HTTP_CONTEXT_HEADER)).build())
                    .ifPresent(lraId -> LRAThreadContext.get().lra(lraId));

            // select current lra annotation handler and process
            AnnotationHandler handler = AnnotationHandler.create(method, coordinatorClient);
            if (handler == null) return;
            handler.handleJaxrsBefore(requestContext, resourceInfo);

        } catch (WebApplicationException e) {
            // Rethrow error responses
            throw e;
        } catch (Throwable e) {
            LOGGER.log(Level.SEVERE, "Error when invoking LRA participant request", e);
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) throws IOException {
        try {
            Method method = resourceInfo.getResourceMethod();
            if (method == null) {
                return;
            }

            // select current lra annotation handler and process
            AnnotationHandler handler = AnnotationHandler.create(method, coordinatorClient);
            if (handler == null) return;
            handler.handleJaxrsAfter(requestContext, responseContext, resourceInfo);

            // Clean up
            LRAThreadContext.clear();
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "After LRA filter", t);
        }
    }
}
