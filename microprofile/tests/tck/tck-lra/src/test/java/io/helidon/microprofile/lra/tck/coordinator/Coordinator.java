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
package io.helidon.microprofile.lra.tck.coordinator;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
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

import io.helidon.microprofile.scheduling.FixedRate;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

@ApplicationScoped
@Path("lra-coordinator")
public class Coordinator {

    public static final String CLIENT_ID_PARAM_NAME = "ClientID";
    public static final String TIMELIMIT_PARAM_NAME = "TimeLimit";
    public static final String PARENT_LRA_PARAM_NAME = "ParentLRA";

    LraPersistentRegistry lraPersistentRegistry = new LraPersistentRegistry();
    static String coordinatorURL = "http://localhost:8070/lra-coordinator/";

    public void init(@Observes @Initialized(ApplicationScoped.class) Object init) {
        lraPersistentRegistry.load();
    }

    private void whenApplicationTerminates(@Observes @BeforeDestroyed(ApplicationScoped.class) final Object event) {
        lraPersistentRegistry.save();
    }
    
    @POST
    @Path("start")
    @Produces(MediaType.TEXT_PLAIN)
    public Response startLRA(
            @QueryParam(CLIENT_ID_PARAM_NAME) @DefaultValue("") String clientId,
            @QueryParam(TIMELIMIT_PARAM_NAME) @DefaultValue("0") Long timelimit,
            @QueryParam(PARENT_LRA_PARAM_NAME) @DefaultValue("") String parentLRA,
            @HeaderParam(LRA_HTTP_CONTEXT_HEADER) String parentId) throws WebApplicationException {
        URI lraId = null;
        try {
            String lraUUID = "LRAID" + UUID.randomUUID(); //todo better UUID
            lraId = new URI(coordinatorURL + lraUUID); //todo verify
            if (parentLRA != null && !parentLRA.isEmpty()) {
                LRA parent = lraPersistentRegistry.get(parentLRA.replace(coordinatorURL, ""));  //todo resolve coordinatorUrl here with member coordinatorURL
                if (parent != null) { // todo null would be unexpected and cause to compensate or exit entirely akin to systemexception
                    LRA childLRA = new LRA(lraUUID, new URI(parentLRA));
                    childLRA.setupTimeout(timelimit);
                    lraPersistentRegistry.put(lraUUID, childLRA);
                    parent.addChild(childLRA);
                }
            } else {
                LRA newLra = new LRA(lraUUID);
                newLra.setupTimeout(timelimit);
                lraPersistentRegistry.put(lraUUID, newLra);
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
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
        if (lra == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        if (lra.isReadyToDelete()) {
            // Already time-outed
            return Response.status(Response.Status.GONE).build();
        }
        lra.terminate(false, true);
        return Response.ok().build();
    }

    @PUT
    @Path("{LraId}/cancel")
    public Response cancelLRA(
            @PathParam("LraId") String lraId) throws NotFoundException {
        LRA lra = lraPersistentRegistry.get(lraId);
        if (lra == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        lra.terminate(true, true);
        return Response.ok().build();
    }

    @PUT
    @Path("{LraId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response joinLRAViaBody(
            @PathParam("LraId") String lraIdParam,
            @QueryParam(TIMELIMIT_PARAM_NAME) @DefaultValue("0") long timeLimit,
            @HeaderParam("Link") @DefaultValue("") String compensatorLink,
            String compensatorData) throws NotFoundException {
        LRA lra = lraPersistentRegistry.get(lraIdParam);
        if (lra == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } else {
            if (lra.checkTimeout()) {
                // too late to join
                return Response.status(Response.Status.PRECONDITION_FAILED).build(); // 410 also acceptable/equivalent behavior
            }
        }
        lra.addParticipant(compensatorLink);
        String recoveryUrl = coordinatorURL + lraIdParam;
        try {
            return Response.ok()
                    .entity(recoveryUrl)
                    .location(new URI(recoveryUrl))
                    .header(LRA_HTTP_RECOVERY_HEADER, recoveryUrl)
                    .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @FixedRate(value = 100, timeUnit = TimeUnit.MILLISECONDS)
    public void run() {
        lraPersistentRegistry.stream().forEach(lra -> {
            if (!lra.isProcessing()) {
                if (lra.isReadyToDelete()) {
                    lraPersistentRegistry.remove(lra.lraId);
                } else {
                    doRun(lra);
                }
            }
        });
    }

    private void doRun(LRA lra) {
        String uri = lra.lraId;
        if (lra.isRecovering) {
            lra.trySendStatus();
            if (!lra.areAllInEndState()) {
                lra.terminate(lra.isCancel, false); // this should purge if areAllAfterLRASuccessfullyCalled
            }
            //todo push all of the following into LRA terminate...
            lra.sendAfterLRA(); //this method gates so no need to do check here
            if (lra.areAllInEndState() && (lra.areAnyInFailedState())) {
                lra.sendForget();
                if (lra.areAllAfterLRASuccessfullyCalledOrForgotten()) lraPersistentRegistry.remove(uri);
            }
        } else {
            if (lra.checkTimeout()) {
                lra.terminate(true, false);
            }
        }
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
