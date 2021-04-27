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
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;


@ApplicationScoped
public class TestApplication extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        return Set.of(
                StartAndCloseCdi.class,
                StartAndAfter.class,
                StartAndClose.class,
                DontEnd.class,
                Timeout.class,
                Recovery.class,
                RecoveryStatus.class
        );
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

    @Path("/start-and-close-cdi")
    public static class StartAndCloseCdi extends CommonAfter {

        @Inject
        BasicTest basicTest;

        @PUT
        @LRA(LRA.Type.REQUIRES_NEW)
        @Path("/start")
        public void doInTransaction(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {

        }

        @Complete
        public Response complete(URI lraId) {
            basicTest.getCompletable("start-and-close-cdi").complete(null);
            return LRAResponse.completed();
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

    @ApplicationScoped
    @Path(RecoveryStatus.PATH_BASE)
    public static class RecoveryStatus {

        static final String PATH_BASE = "recovery-status";
        static final String PATH_START_LRA = "start-compensate";
        static final String CS_START_LRA = PATH_BASE + PATH_START_LRA;
        static final String CS_COMPENSATE_FIRST = CS_START_LRA + "compensate-first";
        static final String CS_COMPENSATE_SECOND = CS_START_LRA + "compensate-second";
        static final String CS_STATUS = CS_START_LRA + "status";
        static final String CS_EXPECTED_STATUS = CS_STATUS + "expected";

        @Inject
        BasicTest basicTest;

        @PUT
        @Path(PATH_START_LRA)
        @LRA(value = LRA.Type.REQUIRES_NEW)
        public Response startCompensateLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                           @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId,
                                           ParticipantStatus reportStatus) {
            basicTest.getCompletable(CS_START_LRA, lraId).complete(lraId);
            basicTest.getCompletable(CS_EXPECTED_STATUS, lraId).complete(reportStatus);
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
        public Response compensateLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                                      @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId) {

            CompletableFuture<URI> completable = basicTest.getCompletable(CS_COMPENSATE_FIRST, lraId);
            boolean secondCall = completable.isDone();
            completable.complete(lraId);
            if (secondCall) {
                basicTest.getCompletable(CS_COMPENSATE_SECOND, lraId).complete(lraId);
            } else {
                try {
                    // sleep longer than coordinator waits for compensate response
                    // to force it to use @Status
                    // TODO: get timeout from mock coordinator
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return LRAResponse.failedToComplete();
            }
            return LRAResponse.compensated();
        }

        @Status
        public ParticipantStatus status(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {
            basicTest.getCompletable(CS_STATUS, lraId).complete(lraId);
            // we slept thru the first compensate call, let coordinator know if we want it to try compensate again
            // retrieve saved status from #startCompensateLRA
            return basicTest.await(CS_EXPECTED_STATUS, lraId);
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
