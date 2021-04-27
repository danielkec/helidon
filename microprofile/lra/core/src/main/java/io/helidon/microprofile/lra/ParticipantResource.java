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

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.lra.LRAResponse;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

@ApplicationScoped
@Path("lra-client-cdi-methods")
public class ParticipantResource {
    //http://127.0.0.1:43733/lra-client-cdi-methods/complete/io.helidon.microprofile.lra.TestApplication$StartAndCloseCdi/complete
    @Inject
    private ParticipantService participantService;

    @PUT
    @Path("/compensate/{fqdn}/{methodName}")
    public Response compensate(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                               @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId,
                               @PathParam("fqdn") String fqdn,
                               @PathParam("methodName") String methodName) {
        try {
            Object result = participantService.invoke(fqdn, methodName, lraId, recoveryId);
            if (result instanceof Response) {
                return (Response) result;
            } else {
                return Response.ok(result).build();
            }
        } catch (InvocationTargetException e) {
            return LRAResponse.completed();
        }
    }

    @PUT
    @Path("/complete/{fqdn}/{methodName}")
    public CompletionStage<Response> complete(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                              @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId,
                                              @PathParam("fqdn") String fqdn,
                                              @PathParam("methodName") String methodName) {
        try {
            Object result = participantService.invoke(fqdn, methodName, lraId, recoveryId);
            if (result instanceof CompletionStage) {
                return (CompletionStage<Response>) result;
            } else {
                return CompletableFuture.completedFuture((Response) result);
            }
        } catch (InvocationTargetException e) {
            return CompletableFuture.completedFuture(LRAResponse.completed());
        }
    }

    @PUT
    @Path("/afterlra/{fqdn}/{methodName}")
    public Response after(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                          @PathParam("fqdn") String fqdn,
                          @PathParam("methodName") String methodName,
                          LRAStatus status) {
        try {
            participantService.invoke(fqdn, methodName, lraId, status);
            return Response.ok().build();
        } catch (InvocationTargetException e) {
            return Response.ok().build();
        }
    }

    @GET
    @Path("/status/{fqdn}/{methodName}")
    public Response status(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                           @PathParam("fqdn") String fqdn,
                           @PathParam("methodName") String methodName) {
        try {
            ParticipantStatus result = (ParticipantStatus) participantService.invoke(fqdn, methodName, lraId, null);
            if(result == null){
                // If the participant has already responded successfully to an @Compensate or @Complete 
                // method invocation then it MAY report 410 Gone HTTP status code 
                // or in the case of non-JAX-RS method returning ParticipantStatus null.
                return Response.status(Response.Status.GONE).build();
            }
            return Response.ok(result.name()).build();
        } catch (InvocationTargetException e) {
            return Response.ok().build();
        }
    }

}
