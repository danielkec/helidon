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
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.lra.LRAResponse;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

@ApplicationScoped
public class TestApplication extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        return Set.of(StartAndAfter.class, StartAndClose.class, DontEnd.class, Timeout.class, Recovery.class);
    }

    @Path("/start-and-close")
    public static class StartAndClose extends CommonAfter {

        @Inject
        BasicTest basicTest;

        @PUT
        @LRA(LRA.Type.REQUIRES_NEW)
        @Path("/start")
        public void doInTransaction(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {
            //Single.timer(100, TimeUnit.MILLISECONDS, executor).await();
        }

        @Complete
        @Path("/complete")
        @PUT
        public Response completeWork(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId, String userData) {
            basicTest.getCompletable("start-and-close").complete(null);
            return Response.ok(ParticipantStatus.Completed.name()).build();
        }
    }

    @ApplicationScoped
    @Path("/start-and-after")
    public static class StartAndAfter extends CommonAfter {

        @PUT
        @LRA(LRA.Type.REQUIRES_NEW)
        @Path("/start")
        public void doInTransaction(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {
            //Single.timer(100, TimeUnit.MILLISECONDS, executor).await();
        }

    }

    @ApplicationScoped
    @Path("/dont-end")
    public static class DontEnd extends CommonAfter {

        @Inject
        BasicTest basicTest;

        @PUT
        @Path("first-not-ending")
        @LRA(value = LRA.Type.REQUIRES_NEW, end = false)
        public Response startDontEndLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {
            basicTest.getCompletable("first-not-ending").complete(lraId);
            return Response.ok().build();
        }

        @PUT
        @Path("second-ending")
        @LRA(value = LRA.Type.MANDATORY, end = true)
        public Response endLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {
            basicTest.getCompletable("second-ending").complete(lraId);
            return Response.ok().build();
        }

    }

    @ApplicationScoped
    @Path("/timeout")
    public static class Timeout {

        @Inject
        BasicTest basicTest;

        @PUT
        @Path("timeout")
        @LRA(value = LRA.Type.REQUIRES_NEW, timeLimit = 500, timeUnit = ChronoUnit.MILLIS)
        public Response startLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            basicTest.getCompletable("timeout").complete(lraId);
            return Response.ok().build();
        }

        @PUT
        @Path("/compensate")
        @Produces(MediaType.APPLICATION_JSON)
        @Compensate
        public Response compensateWork(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                       @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId) {
            basicTest.getCompletable("timeout-compensated").complete(null);
            return LRAResponse.compensated();
        }
    }

    @ApplicationScoped
    @Path("/recovery")
    public static class Recovery {

        @Inject
        BasicTest basicTest;

        @PUT
        @Path("start")
        @LRA(value = LRA.Type.REQUIRES_NEW, timeLimit = 500, timeUnit = ChronoUnit.MILLIS)
        public Response startLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                 @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId) {
            // Force to compensate
            return Response.serverError()
                    .header(LRA_HTTP_CONTEXT_HEADER, lraId.toASCIIString())
                    .header(LRA_HTTP_RECOVERY_HEADER, recoveryId.toASCIIString())
                    .build();
        }

        @PUT
        @Path("/compensate")
        @Produces(MediaType.APPLICATION_JSON)
        @Compensate
        public Response compensateWork(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
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
    }

    @ApplicationScoped
    public static class CommonAfter {

        @Inject
        BasicTest basicTest;

        @AfterLRA
        @Path("/after")
        @PUT
        public Response completeWork(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId, String userData) {
            basicTest.getCompletable("start-and-after").complete(null);
            return Response.ok(ParticipantStatus.Completed.name()).build();
        }
    }
}
