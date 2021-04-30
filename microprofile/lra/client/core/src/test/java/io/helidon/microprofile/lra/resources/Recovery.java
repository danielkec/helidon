/*
 * Copyright (c) 2021 Oracle and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.helidon.microprofile.lra.resources;

import org.eclipse.microprofile.lra.LRAResponse;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.helidon.microprofile.lra.BasicTest;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

@ApplicationScoped
@Path("/recovery")
public class Recovery {

    @Inject
    BasicTest basicTest;

    @PUT
    @Path("start-compensate")
    @LRA(value = LRA.Type.REQUIRES_NEW, timeLimit = 500, timeUnit = ChronoUnit.MILLIS)
    public Response startCompensateLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                       @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId) {
        // Force to compensate
        return Response.serverError()
                .header(LRA_HTTP_CONTEXT_HEADER, lraId.toASCIIString())
                .header(LRA_HTTP_RECOVERY_HEADER, recoveryId.toASCIIString())
                .build();
    }

    @PUT
    @Path("start-complete")
    @LRA(value = LRA.Type.REQUIRES_NEW, timeLimit = 500, timeUnit = ChronoUnit.MILLIS)
    public Response startCompleteLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                     @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId) {
        return Response.ok()
                .header(LRA_HTTP_CONTEXT_HEADER, lraId.toASCIIString())
                .header(LRA_HTTP_RECOVERY_HEADER, recoveryId.toASCIIString())
                .build();
    }

    @PUT
    @Path("/compensate")
    @Produces(MediaType.APPLICATION_JSON)
    @Compensate
    public Response compensateLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                  @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId) {

        CompletableFuture<URI> completable = basicTest.getCompletable("recovery-compensated-first");
        boolean secondCall = completable.isDone();
        completable.complete(lraId);
        if (secondCall) {
            basicTest.getCompletable("recovery-compensated-second").complete(lraId);
        } else {
            return LRAResponse.failedToCompensate();
        }
        return LRAResponse.compensated();
    }

    @PUT
    @Path("/complete")
    @Produces(MediaType.APPLICATION_JSON)
    @Complete
    public Response completeLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId) {

        CompletableFuture<URI> completable = basicTest.getCompletable("recovery-completed-first");
        boolean secondCall = completable.isDone();
        completable.complete(lraId);
        if (secondCall) {
            basicTest.getCompletable("recovery-completed-second").complete(lraId);
        } else {
            return LRAResponse.failedToComplete();
        }
        return LRAResponse.completed();
    }
}
