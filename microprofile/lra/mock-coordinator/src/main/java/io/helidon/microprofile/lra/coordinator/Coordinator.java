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
package io.helidon.microprofile.lra.coordinator;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import io.helidon.common.reactive.Single;
import io.helidon.microprofile.scheduling.FixedRate;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

@ApplicationScoped
@Path("lra-coordinator")
public class Coordinator {

    public static final String CLIENT_ID_PARAM_NAME = "ClientID";
    public static final String TIMELIMIT_PARAM_NAME = "TimeLimit";
    public static final String PARENT_LRA_PARAM_NAME = "ParentLRA";

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class.getName());

    LraPersistentRegistry lraPersistentRegistry = new LraPersistentRegistry();

    @Inject
    @ConfigProperty(name = "lra.tck.coordinator.persist", defaultValue = "false")
    private Boolean persistent;

    @Inject
    @ConfigProperty(name = "mp.lra.coordinator.url", defaultValue = "http://localhost:8070/lra-coordinator")
    private String coordinatorURL;

    public void init(@Observes @Initialized(ApplicationScoped.class) Object init) {
        if (persistent) {
            lraPersistentRegistry.load();
        }
    }

    private void whenApplicationTerminates(@Observes @BeforeDestroyed(ApplicationScoped.class) final Object event) {
        if (persistent) {
            lraPersistentRegistry.save();
        }
    }

    @POST
    @Path("start")
    @Produces(MediaType.TEXT_PLAIN)
    public Response startLRA(
            @QueryParam(CLIENT_ID_PARAM_NAME) @DefaultValue("") String clientId,
            @QueryParam(TIMELIMIT_PARAM_NAME) @DefaultValue("0") Long timelimit,
            @QueryParam(PARENT_LRA_PARAM_NAME) @DefaultValue("") String parentLRA,
            @HeaderParam(LRA_HTTP_CONTEXT_HEADER) String parentId) throws WebApplicationException {

        String lraUUID = "LRAID" + UUID.randomUUID(); //todo better UUID
        URI lraId = UriBuilder.fromPath(coordinatorURL).path(lraUUID).build();
        LOGGER.log(Level.INFO, "POST START " + lraId);
        if (parentLRA != null && !parentLRA.isEmpty()) {
            LRA parent = lraPersistentRegistry.get(parentLRA.replace(coordinatorURL, ""));  //todo resolve coordinatorUrl here with member coordinatorURL
            if (parent != null) { // todo null would be unexpected and cause to compensate or exit entirely akin to systemexception
                LRA childLRA = new LRA(lraUUID, UriBuilder.fromPath(parentLRA).build());
                childLRA.setupTimeout(timelimit);
                lraPersistentRegistry.put(lraUUID, childLRA);
                parent.addChild(childLRA);
            }
        } else {
            LRA newLra = new LRA(lraUUID);
            newLra.setupTimeout(timelimit);
            lraPersistentRegistry.put(lraUUID, newLra);
        }
        return Response.created(lraId)
                .entity(lraId.toString())
                .header(LRA_HTTP_CONTEXT_HEADER, lraId)
                .build();
    }

    @PUT
    @Path("{LraId}/close")
    @Produces(MediaType.TEXT_PLAIN)
    public Response closeLRA(
            @PathParam("LraId") String lraId) throws NotFoundException {
        LRA lra = lraPersistentRegistry.get(lraId);
        LOGGER.log(Level.INFO, "PUT CLOSE " + lraId + " " + lra);
        if (lra == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        if (lra.status().get() != LRAStatus.Active) {
            // Already time-outed
            return Response.status(Response.Status.GONE).build();
        }
        tick();
        lra.close();
        tick();
        return Response.ok().build();
    }

    @PUT
    @Path("{LraId}/cancel")
    public Response cancelLRA(
            @PathParam("LraId") String lraId) throws NotFoundException {
        LRA lra = lraPersistentRegistry.get(lraId);
        LOGGER.log(Level.INFO, "PUT CANCEL " + lraId + " " + lra);
        if (lra == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        tick();
        lra.cancel();
        return Response.ok().build();
    }

    @PUT
    @Path("{LraId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response join(
            @PathParam("LraId") String lraId,
            @QueryParam(TIMELIMIT_PARAM_NAME) @DefaultValue("0") long timeLimit,
            @HeaderParam("Link") @DefaultValue("") String compensatorLink,
            String compensatorData) throws NotFoundException {
        LRA lra = lraPersistentRegistry.get(lraId);
        LOGGER.log(Level.INFO, "PUT JOIN " + lraId + " " + lra);
        if (lra == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } else {
            if (lra.checkTimeout()) {
                // too late to join
                return Response.status(Response.Status.PRECONDITION_FAILED).build(); // 410 also acceptable/equivalent behavior
            }
        }
        lra.addParticipant(compensatorLink);
        String recoveryUrl = coordinatorURL + lraId;
        try {
            return Response.ok()
                    .entity(recoveryUrl)
                    .location(new URI(recoveryUrl))
                    .header(LRA_HTTP_RECOVERY_HEADER, recoveryUrl)
                    .build();
        } catch (URISyntaxException e) {
            LOGGER.log(Level.SEVERE, "Error when joining LRA " + lraId, e);
            throw new RuntimeException(e);
        }
    }


    @GET
    @Path("{LraId}/status")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getStatus(@PathParam("LraId") String lraId) {
        LRA lra = lraPersistentRegistry.get(lraId);
        LOGGER.log(Level.INFO, "GET STATUS " + lraId + " " + lra);
        if (lra == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .build();
        }

        return Response.ok()
                .entity(lra.status().get().name())
                .build();
    }

    @GET
    @Path("recovery")
    @Produces(MediaType.TEXT_PLAIN)
    public Response recovery() {
        var recoverables = Set.of(LRAStatus.Cancelling, LRAStatus.Closing, LRAStatus.Active);
        return Coordinator.nextRecoveryCycle()
                .map(String::valueOf)
                .onCompleteResume(lraPersistentRegistry
                        .stream()
                        .filter(lra -> recoverables.contains(lra.status().get()))
                        .map(lra -> lra.status().get().name() + "-" + lra.lraId)
                        .collect(Collectors.joining(","))
                ).map(s -> Response.ok(s).build())
                .first()
                .await();
    }

    @FixedRate(value = 500, timeUnit = TimeUnit.MILLISECONDS)
    public void tick() {
        lraPersistentRegistry.stream().forEach(lra -> {
            if (lra.isReadyToDelete()) {
                lraPersistentRegistry.remove(lra.lraId);
            } else {
                synchronized (this) {
                    if (LRAStatus.Cancelling == lra.status().get()) {
                        LOGGER.log(Level.INFO, "Recovering {0}", lra.lraId);
                        lra.cancel();
                    }
                    if (LRAStatus.Closing == lra.status().get()) {
                        LOGGER.log(Level.INFO, "Recovering {0}", lra.lraId);
                        lra.close();
                    }
                    if (lra.checkTimeout() && lra.status().get().equals(LRAStatus.Active)) {
                        LOGGER.log(Level.INFO, "Timeouting {0} ", lra.lraId);
                        lra.terminate();
                    }
                    if (Set.of(LRAStatus.Closed, LRAStatus.Cancelled).contains(lra.status().get())) {
                        // If a participant is unable to complete or compensate immediately or because of a failure
                        // then it must remember the fact (by reporting its' status via the @Status method) 
                        // until explicitly told that it can clean up using this @Forget annotation.
                        LOGGER.log(Level.INFO, "Forgetting {0} ", lra.lraId);
                        lra.tryForget();
                        lra.forgetNested();
                    }
                }
            }
        });
        completedRecovery.getAndSet(new CompletableFuture<>()).complete(null);
    }

    static AtomicReference<CompletableFuture<Void>> completedRecovery = new AtomicReference<>(new CompletableFuture<>());

    public static Single<Void> nextRecoveryCycle() {
        return Single.create(completedRecovery.get(), true)
                //wait for the second one, as first could have been in progress
                .onCompleteResumeWith(Single.create(completedRecovery.get(), true))
                .ignoreElements();
    }

    @PUT
    @Path("{LraId}/remove")
    @Produces(MediaType.APPLICATION_JSON)
    public Response leaveLRA(@PathParam("LraId") String lraId, String compensatorUrl) {
        LRA lra = lraPersistentRegistry.get(lraId);
        if (lra == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        lra.removeParticipant(compensatorUrl);
        return Response.ok().build();
    }

}
