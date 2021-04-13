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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

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
import javax.xml.bind.JAXBException;

import io.helidon.common.reactive.CompletionAwaitable;
import io.helidon.common.reactive.Single;
import io.helidon.microprofile.scheduling.FixedRate;
import io.helidon.microprofile.scheduling.Scheduled;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
@Path("lra-coordinator")
public class Coordinator {
    private static final Logger LOGGER = Logger.getLogger(Coordinator.class.getName());

    public static final String STATUS_PARAM_NAME = "Status";
    public static final String CLIENT_ID_PARAM_NAME = "ClientID";
    public static final String TIMELIMIT_PARAM_NAME = "TimeLimit";
    public static final String PARENT_LRA_PARAM_NAME = "ParentLRA";

    LraPersistentRegistry lraPersistentRegistry = new LraPersistentRegistry();
    static String coordinatorURL = "http://localhost:8070/lra-coordinator/";

    public void init(@Observes @Initialized(ApplicationScoped.class) Object init) throws JAXBException, IOException {

        lraPersistentRegistry.load();
        
        Config config = ConfigProvider.getConfig();
        LOGGER.info("Coordinator init config.getValue(\"lra.coordinator.url\"):" + config.getOptionalValue("lra.coordinator.url", String.class));
// todo this or lra.coordinator.url override...  coordinatorURL = "http://" + config.getValue("server.host", String.class) + ":" + config.getValue("server.port", String.class) + "/lra-coordinator/";
        LOGGER.info("Coordinator init coordinatorURL:" + coordinatorURL);
    }

    private void whenApplicationTerminates(@Observes @BeforeDestroyed(ApplicationScoped.class) final Object event) throws JAXBException, IOException {
        lraPersistentRegistry.save();
    }


    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)

    public List<String> getAllLRAs(
            @QueryParam(STATUS_PARAM_NAME) @DefaultValue("") String state) {
        ArrayList<String> lraStrings = new ArrayList<>();
        lraStrings.add("testlraid");
        return lraStrings;
    }

    @GET
    @Path("{LraId}/status")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getLRAStatus(
            @PathParam("LraId") String lraId,
            @QueryParam("effectivelyActive") @DefaultValue("false") boolean isEffectivelyActive) throws NotFoundException {
        return Response.noContent().build(); // 204 meaning the LRA is still active
    }

    @GET
    @Path("{LraId}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getLRAInfo(
            @PathParam("LraId") String lraId) throws NotFoundException {
        return "test";
    }

    @GET
    @Path("/status/{LraId}")
    @Produces(MediaType.TEXT_PLAIN)
    public Boolean isActiveLRA(
            @PathParam("LraId") String lraId) throws NotFoundException {
        return true; //todo
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
            String lraUUID = "LRAID" + UUID.randomUUID().toString(); //todo better UUID
            lraId = new URI(coordinatorURL + lraUUID); //todo verify
            String rootParentOrChild = "parent(root)";
            if (parentLRA != null && !parentLRA.isEmpty()) {
                LRA parent = lraPersistentRegistry.get(parentLRA.replace(coordinatorURL, ""));  //todo resolve coordinatorUrl here with member coordinatorURL
                if (parent != null) { // todo null would be unexpected and cause to compensate or exit entirely akin to systemexception
                    LRA childLRA = new LRA(lraUUID, new URI(parentLRA));
                    childLRA.setupTimeout(timelimit);
                    lraPersistentRegistry.put(lraUUID, childLRA);
                    parent.addChild(childLRA);
                    rootParentOrChild = "nested(" + childLRA.nestingDetail() + ")";
                }
            } else {
                LRA newLra = new LRA(lraUUID);
                newLra.setupTimeout(timelimit);
                lraPersistentRegistry.put(lraUUID, newLra);
            }
            log("[start] " + rootParentOrChild + " clientId = " + clientId + ", timelimit = " + timelimit +
                    ", parentLRA = " + parentLRA + ", parentId = " + parentId + " lraId:" + lraId);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return Response.created(lraId)
                .entity(lraId.toString())
                .header(LRA_HTTP_CONTEXT_HEADER,
                        ThreadContext.getContexts().size() == 1 ? ThreadContext.getContexts().get(0) : ThreadContext.getContexts())
                .build();
    }

    @PUT
    @Path("{LraId}/renew")
    public Response renewTimeLimit(
            @QueryParam(TIMELIMIT_PARAM_NAME) @DefaultValue("0") Long timelimit,
            @PathParam("LraId") String lraId) throws NotFoundException {

        return Response.status(400).build();
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
        log("[close] " + getParentChildDebugString(lra) + " lraId:" + lraId);
        lra.terminate(false, true);
        return Response.ok().build();
    }

    @PUT
    @Path("{LraId}/cancel")
    public Response cancelLRA(
            @PathParam("LraId") String lraId) throws NotFoundException {
        LRA lra = lraPersistentRegistry.get(lraId);
        log("[cancel] " + getParentChildDebugString(lra) + " lraId:" + lraId);
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
        URI lraId = toURI(lraIdParam);
        int status = Response.Status.OK.getStatusCode();
        String lraIdString = lraId.toString().substring(lraId.toString().indexOf("LRAID"));
        LRA lra = lraPersistentRegistry.get(lraIdString);
        if (lra == null) {
            log("[join] lraRecord == null for lraIdString:" + lraIdString +
                    "lraRecordMap.size():" + lraPersistentRegistry.size());
            return Response.status(Response.Status.NOT_FOUND).build(); //todo this is actually error
        } else {
            if (lra.checkTimeout()) {
                log("[join] expired");
                // too late to join
                return Response.status(Response.Status.PRECONDITION_FAILED).build(); // 410 also acceptable/equivalent behavior
            }
        }
        if (compensatorData == null || compensatorData.trim().equals("")) {
            log("[join] no compensatorLink information");
        }
        String debugString = lra.addParticipant(compensatorLink);
        log("[join] " + debugString + " to " + getParentChildDebugString(lra) +
                " lraIdParam = " + lraIdParam + ", timeLimit = " + timeLimit);
        String recoveryUrl = coordinatorURL + lraIdString;
        try {
            return Response.status(status)
                    .entity(recoveryUrl)
                    .location(new URI(recoveryUrl))
                    .header(LRA_HTTP_RECOVERY_HEADER, recoveryUrl)
                    .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
//    @Scheduled(value = "0/5 * * * * ? *")
    @FixedRate(value = 100, timeUnit = TimeUnit.MILLISECONDS)
    public void run() {
        lraPersistentRegistry.stream().forEach(lra -> {
            if (!lra.isProcessing()) {
                doRun(lra); //todo add exponential backoff
            }
        });
        completedRecovery.getAndSet(new CompletableFuture<>()).complete(null);
    }
    
    static AtomicReference<CompletableFuture<Void>> completedRecovery = new AtomicReference<>(new CompletableFuture<>()); 
    
    public static Single<Void> waitForNextRecoveryCycle(){
        return Single.create(completedRecovery.get(), true)
                //wait for the second one, as first could have been in progress
                .onCompleteResumeWith(Single.create(completedRecovery.get(), true))
                .ignoreElements();
    } 

    private void doRun(LRA lra) {
        String uri = lra.lraId;
        if (lra.isReadyToDelete()) {
            lraPersistentRegistry.remove(uri);
        } else if (lra.isRecovering) {
            if (lra.hasStatusEndpoints()) lra.sendStatus();
            if (!lra.areAllInEndState()) {
                lra.terminate(lra.isCancel, false); // this should purge if areAllAfterLRASuccessfullyCalled
            }
            //todo push all of the following into LRA terminate...
            lra.sendAfterLRA(); //this method gates so no need to do check here
            if (lra.areAllInEndState() && (lra.areAnyInFailedState())) { // || (lra.isChild && lra.isUnilateralCallIfNested && lra.isCancel == false)
                lra.sendForget();
                if (lra.areAllAfterLRASuccessfullyCalledOrForgotten()) {
                    if (lra.areAllAfterLRASuccessfullyCalledOrForgotten()) lraPersistentRegistry.remove(uri);
                }
            }
        } else {
            if (lra.checkTimeout()) {
//                log("[timeout], will end uri:" + uri +
//                        " timeout:" + lra.timeout + " currentTime:" + currentTime +
//                        " ms over:" + (currentTime - lra.timeout));
                lra.terminate(true, false);
            }
        }
    }

    @PUT
    @Path("{LraId}/remove")
    @Produces(MediaType.APPLICATION_JSON)
    public Response leaveLRA(
            @PathParam("LraId") String lraId,
            String compensatorUrl) throws NotFoundException {
        String lraIdString = lraId.substring(lraId.indexOf("LRAID"));
        LRA lra = lraPersistentRegistry.get(lraIdString);
        if (lra != null) {
            lra.removeParticipant(compensatorUrl, false, true);
        }
        int status = 200;
        return Response.status(status).build();
    }

    private URI toURI(String lraId) {
        return toURI(lraId, "Invalid LRA id format");
    }

    private URI toURI(String lraId, String message) {
        URL url;
        try {
            return new URL(lraId).toURI();
        } catch (Exception e) {
            try {
//                url = new URL(String.format("%s%s/%s", context.getBaseUri(), COORDINATOR_PATH_NAME, lraId));
                url = new URL(coordinatorURL + lraId);
            } catch (MalformedURLException e1) {
                throw new RuntimeException("todo paul runtime exception badrequest in toURI " + e1);
            }
        }
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException("todo paul runtime exception URISyntaxException in toURI " + e);
        }
    }

    private String getParentChildDebugString(LRA lra) {
        return lra == null ? null : (lra.isParent ? "parent" : "") + (lra.isParent && lra.isChild ? " and " : "") +
                (lra.isChild ? "child" : "") + (!lra.isParent && !lra.isChild ? "currently flat LRA" : "");
    }

    void log(String message) {
        LOGGER.info("[coordinator]" + message);
    }

}
