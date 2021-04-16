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

import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Optional;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.ws.rs.ConstrainedTo;
import javax.ws.rs.RuntimeType;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

@ConstrainedTo(RuntimeType.SERVER)
public class JaxrsMethodFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOGGER = Logger.getLogger(JaxrsMethodFilter.class.getName());

    @Context
    protected ResourceInfo resourceInfo;

    @Inject
    CoordinatorClient coordinatorClient;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        URI baseUri = requestContext.getUriInfo().getBaseUri();
        Method method = resourceInfo.getResourceMethod();
        LRA lraAnnotation = Optional.ofNullable(method.getAnnotation(LRA.class))
                .orElseGet(() -> method.getDeclaringClass().getAnnotation(LRA.class));

        if(lraAnnotation == null) return;
        
        requestContext.setProperty("lra.end", lraAnnotation.end());

        switch (lraAnnotation.value()) {
            case REQUIRES_NEW:
                URI lraId = coordinatorClient.start(null, method.getDeclaringClass().getName() + "#" + method.getName(), lraAnnotation.timeLimit());
                LOGGER.info("Coordinator confirmed started LRA " + lraId);
                coordinatorClient.join(lraId,
                        lraAnnotation.timeLimit(),
                        null,
                        UriBuilder.fromUri(baseUri).path("start-and-close").path("complete").build(),
                        null, null, null, null, null
                );
                requestContext.setProperty("lra.id", lraId);
                break;
            default:
                LOGGER.severe("Unsupported LRA type " + lraAnnotation.value() + " on method " + method.getName());
                requestContext.abortWith(Response.status(500).build());
        }

    }

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) throws IOException {
        URI lraId = (URI) requestContext.getProperty("lra.id");
        if (lraId != null && (boolean) requestContext.getProperty("lra.end")) {
            LOGGER.info("Closing LRA " + lraId);
            coordinatorClient.close(lraId);
        }
    }
}
