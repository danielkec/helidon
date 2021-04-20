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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ResourceInfo;

import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;
import org.jboss.jandex.AnnotationInstance;

interface AnnotationHandler {

    Logger LOGGER = Logger.getLogger(AnnotationHandler.class.getName());

    Map<String, HandlerMaker> HANDLER_SUPPLIERS =
            Map.of(
                    LRA.class.getName(), LRAAnnotationHandler::new,
                    Leave.class.getName(), (a, client, i) -> new LeaveAnnotationHandler(client)
            );

    static List<AnnotationHandler> create(Method m, InspectionService inspectionService, CoordinatorClient coordinatorClient) {
        Set<AnnotationInstance> lraAnnotations = inspectionService.lookUpLraAnnotations(m);
        if (lraAnnotations.isEmpty()) {
            return List.of(new NoAnnotationHandler());
        }

        return lraAnnotations.stream().map(lraAnnotation -> {
            var handlerMaker =
                    HANDLER_SUPPLIERS.get(lraAnnotation.name().toString());

            if (handlerMaker == null) {
                // Non LRA annotation on LRA method, skipping
                return null;
            }
            return handlerMaker.make(lraAnnotation, coordinatorClient, inspectionService);
        }).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    void handleJaxrsBefore(ContainerRequestContext requestContext,
                           ResourceInfo resourceInfo);

    void handleJaxrsAfter(ContainerRequestContext requestContext,
                          ContainerResponseContext responseContext,
                          ResourceInfo resourceInfo);

    @FunctionalInterface
    interface HandlerMaker {
        AnnotationHandler make(AnnotationInstance annotationInstance,
                               CoordinatorClient coordinatorClient,
                               InspectionService inspectionService);
    }
}
