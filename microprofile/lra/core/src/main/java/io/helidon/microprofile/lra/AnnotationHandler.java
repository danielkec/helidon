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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.logging.Logger;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ResourceInfo;

import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;

interface AnnotationHandler {

    static final Logger LOGGER = Logger.getLogger(AnnotationHandler.class.getName());

    static Map<Class<? extends Annotation>, BiFunction<Annotation, CoordinatorClient, AnnotationHandler>> HANDLER_SUPPLIERS =
            Map.of(
                    LRA.class, (a, client) -> new LRAAnnotationHandler((LRA) a, client),
                    Leave.class, (a, client) -> new LeaveAnnotationHandler(client)
            );

    static AnnotationHandler create(Method m, CoordinatorClient coordinatorClient) {
        Optional<Annotation> lraAnnotation = Participant.getLRAAnnotation(m);
        if (lraAnnotation.isEmpty()) {
            return new NoAnnotationHandler();
        }

        var handlerMaker = HANDLER_SUPPLIERS.get(lraAnnotation.get().annotationType());

        if (handlerMaker == null) {
            // TODO: this can't happen
            LOGGER.severe("Not implemented yet!!! Not implemented handler for LRA annoration "
                    + lraAnnotation.get().annotationType().getName());
            return null;
        }

        return handlerMaker.apply(lraAnnotation.get(), coordinatorClient);
    }

    void handleJaxrsBefore(ContainerRequestContext requestContext,
                           ResourceInfo resourceInfo);

    void handleJaxrsAfter(ContainerRequestContext requestContext,
                          ContainerResponseContext responseContext,
                          ResourceInfo resourceInfo);
}
