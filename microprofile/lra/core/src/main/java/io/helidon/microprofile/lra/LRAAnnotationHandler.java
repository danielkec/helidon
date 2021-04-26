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
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Response;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.jboss.jandex.AnnotationInstance;

class LRAAnnotationHandler implements AnnotationHandler {

    private static final Logger LOGGER = Logger.getLogger(LRAAnnotationHandler.class.getName());

    private final InspectionService.Lra annotation;
    private final CoordinatorClient coordinatorClient;
    private ParticipantService participantService;

    LRAAnnotationHandler(AnnotationInstance annotation, 
                         CoordinatorClient coordinatorClient, 
                         InspectionService inspectionService,
                         ParticipantService participantService) {
        this.participantService = participantService;
        this.annotation = inspectionService.lraAnnotation(annotation);
        this.coordinatorClient = coordinatorClient;
    }

    @Override
    public void handleJaxrsBefore(ContainerRequestContext reqCtx, ResourceInfo resourceInfo) {
        var method = resourceInfo.getResourceMethod();
        var baseUri = reqCtx.getUriInfo().getBaseUri();
        var participant = participantService.participant(baseUri, resourceInfo.getResourceClass());
        var existingLraId = LRAThreadContext.get().lra();
        var timeLimit = annotation.timeLimit();
        var end = annotation.end();

        URI lraId = null;
        switch (annotation.value()) {
            case NESTED:
                if (existingLraId.isPresent()) {
                    reqCtx.getHeaders().putSingle(LRA_HTTP_PARENT_CONTEXT_HEADER, existingLraId.get().toASCIIString());
                    reqCtx.setProperty("suppressed.lra", existingLraId.get());
                    lraId = coordinatorClient.start(existingLraId.get(), method.getDeclaringClass().getName() + "#" + method.getName(), timeLimit);
                    LOGGER.info("Coordinator confirmed started LRA " + lraId);
                    URI recoveryUri = coordinatorClient.join(lraId, timeLimit, participant);
                    reqCtx.getHeaders().add(LRA_HTTP_RECOVERY_HEADER, recoveryUri.toASCIIString());
                } else {
                    lraId = coordinatorClient.start(null, method.getDeclaringClass().getName() + "#" + method.getName(), timeLimit);
                    LOGGER.info("Coordinator confirmed started LRA " + lraId);
                    URI recoveryUri = coordinatorClient.join(lraId, timeLimit, participant);
                    reqCtx.getHeaders().add(LRA_HTTP_RECOVERY_HEADER, recoveryUri.toASCIIString());
                }
                break;
            case NEVER:
                if (existingLraId.isPresent()) {
                    // If called inside an LRA context, i.e., the method is not executed 
                    // and a 412 Precondition Failed is returned
                    reqCtx.abortWith(Response.status(Response.Status.PRECONDITION_FAILED).build());
                    return;
                }
                break;
            case NOT_SUPPORTED:
                reqCtx.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
                return;
            case SUPPORTS:
                if (existingLraId.isPresent()) {
                    URI recoveryUri = coordinatorClient.join(existingLraId.get(), timeLimit, participant);
                    reqCtx.getHeaders().add(LRA_HTTP_RECOVERY_HEADER, recoveryUri.toASCIIString());
                    lraId = existingLraId.get();
                    break;
                }
                break;
            case MANDATORY:
                if (existingLraId.isEmpty()) {
                    // If called outside an LRA context the method is not executed and a
                    // 412 Precondition Failed HTTP status code is returned to the caller
                    reqCtx.abortWith(Response.status(Response.Status.PRECONDITION_FAILED).build());
                    return;
                }
                // existing lra, fall thru to required
            case REQUIRED:
                if (existingLraId.isPresent()) {
                    URI recoveryUri = coordinatorClient.join(existingLraId.get(), timeLimit, participant);
                    reqCtx.getHeaders().add(LRA_HTTP_RECOVERY_HEADER, recoveryUri.toASCIIString());
                    lraId = existingLraId.get();
                    break;
                }
                // non existing lra, fall thru to requires_new
            case REQUIRES_NEW:
                lraId = coordinatorClient.start(null, method.getDeclaringClass().getName() + "#" + method.getName(), timeLimit);
                LOGGER.info("Coordinator confirmed started LRA " + lraId);
                URI recoveryUri = coordinatorClient.join(lraId, timeLimit, participant);
                reqCtx.getHeaders().add(LRA_HTTP_RECOVERY_HEADER, recoveryUri.toASCIIString());
                break;
            default:
                LOGGER.severe("Unsupported LRA type " + annotation.value() + " on method " + method.getName());
                reqCtx.abortWith(Response.status(500).build());
        }
        lraId = lraId != null ? lraId : existingLraId.orElse(null);
        if (lraId != null) {
            reqCtx.getHeaders().putSingle(LRA_HTTP_CONTEXT_HEADER, lraId.toASCIIString());
            LRAThreadContext.get().lra(lraId);
        }
    }

    @Override
    public void handleJaxrsAfter(ContainerRequestContext requestContext,
                                 ContainerResponseContext responseContext,
                                 ResourceInfo resourceInfo) {
        Optional<URI> lraId = Optional.ofNullable((URI) requestContext.getProperty("lra.id"))
                .or(() -> LRAThreadContext.get().lra());

        var end = annotation.end();
        var cancelOnFamilies = annotation.cancelOnFamily();
        var cancelOnStatuses = annotation.cancelOn();

        if (lraId.isPresent()
                && (cancelOnFamilies.contains(responseContext.getStatusInfo().getFamily())
                || cancelOnStatuses.contains(responseContext.getStatusInfo().toEnum()))) {
            LOGGER.info("Cancelling LRA " + lraId.get());
            coordinatorClient.cancel(lraId.get());
            LRAThreadContext.clear();
        } else if (lraId.isPresent() && end) {
            LOGGER.info("Closing LRA " + lraId.get());
            coordinatorClient.close(lraId.get());
            LRAThreadContext.clear();
        }
        URI suppressedLra = (URI) requestContext.getProperty("suppressed.lra");
        if (suppressedLra != null){
            responseContext.getHeaders().putSingle(LRA_HTTP_CONTEXT_HEADER, suppressedLra.toASCIIString());
        }
        
        if (lraId.isPresent()) {
            responseContext.getHeaders().putIfAbsent(LRA_HTTP_CONTEXT_HEADER, List.of(lraId.get().toASCIIString()));
        }
    }
}
