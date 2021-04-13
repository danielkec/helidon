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
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import io.helidon.common.configurable.ServerThreadPoolSupplier;

import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;
import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.Completed;
import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.FailedToCompensate;
import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.FailedToComplete;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_ENDED_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Participant {

    /**
     * Participant state model....
     * Active -------------------------------------------------------------------------> Compensating --> FailedToCompensate
     * --> Completing --> FailedToComplete                                       /             --> Compensated
     * --> Completed --> (only if nested can go to compensating) /
     */
    private static final Logger LOGGER = Logger.getLogger(Participant.class.getName());
    private boolean isAfterLRASuccessfullyCalledIfEnlisted;
    private boolean isForgotten;
    private ParticipantStatus participantStatus;
    private URI completeURI; // a method to be executed when the LRA is closed 200, 202, 409, 410
    private URI compensateURI; //  a method to be executed when the LRA is cancelled 200, 202, 409, 410
    private URI afterURI;  // a method that will be reliably invoked when the LRA enters one of the final states 200
    private URI forgetURI; // a method to be executed when the LRA allows participant to clear all associated information 200, 410
    private URI statusURI; // a method that allow user to state status of the participant with regards to a particular LRA 200, 202, 410
    
    @XmlTransient
    private Client client = ClientBuilder.newBuilder().build();

    ParticipantStatus getParticipantStatus() {
        return participantStatus;
    }

    //todo handle out of order messages, ie validate against state machine
    public void setParticipantStatus(ParticipantStatus participantStatus) {
        this.participantStatus = participantStatus;
    }

    public URI getCompleteURI() {
        return completeURI;
    }

    void setCompleteURI(URI completeURI) {
        this.completeURI = completeURI;
    }

    public URI getCompensateURI() {
        return compensateURI;
    }

    void setCompensateURI(URI compensateURI) {
        this.compensateURI = compensateURI;
    }

    public URI getAfterURI() {
        return afterURI;
    }

    void setAfterURI(URI afterURI) {
        this.afterURI = afterURI;
    }

    public URI getForgetURI() {
        return forgetURI;
    }

    void setForgetURI(URI forgetURI) {
        this.forgetURI = forgetURI;
    }

    URI getStatusURI() {
        return statusURI;
    }

    void setStatusURI(URI statusURI) {
        this.statusURI = statusURI;
    }

    protected void setForgotten() {
        this.isForgotten = true;
    }

    boolean isForgotten() {
        return isForgotten;
    }

    public void setAfterLRASuccessfullyCalledIfEnlisted() {
        isAfterLRASuccessfullyCalledIfEnlisted = true;
    }

    public boolean isAfterLRASuccessfullyCalledIfEnlisted() {
        return isAfterLRASuccessfullyCalledIfEnlisted || afterURI == null;
    }

    //A listener is a participant with afterURI endpoint. It may not have a complete or compensate endpoint.
    boolean isListenerOnly() {
        return completeURI == null && compensateURI == null;
    }

    public String toString() {
        return "Participant type:" + getParticipantType() +
                "\n participantStatus:" + participantStatus +
                "\n completeURI:" + completeURI +
                "\n compensateURI:" + compensateURI +
                "\n afterURI:" + afterURI +
                "\n forgetURI:" + forgetURI +
                "\n statusURI:" + statusURI;
    }

    public boolean isInEndStateOrListenerOnly() {
        return participantStatus == FailedToComplete ||
                participantStatus == FailedToCompensate ||
                participantStatus == Completed ||
                participantStatus == Compensated ||
                isListenerOnly();
    }

    public boolean isInEndStateOrListenerOnlyForTerminationType(boolean isCompensate) {
        if (isCompensate) {
            return participantStatus == FailedToCompensate ||
                    participantStatus == Compensated ||
                    isListenerOnly();
        } else {
            return participantStatus == FailedToComplete ||
                    participantStatus == Completed ||
                    isListenerOnly();
        }
    }


    public void logParticipantMessageWithTypeAndDepth(String message, int nestedDepth) {
        LOGGER.info("[" + getParticipantType() + " participant][depth:" + nestedDepth + "] " + message);
    }


    public boolean init() {
        return true;
    }

    String getParticipantType() {
        return "Rest";
    }

    void sendCompleteOrCancel(LRA lra, boolean isCancel) {
        URI endpointURI = isCancel ? getCompensateURI() : getCompleteURI();
        try {
            logParticipantMessageWithTypeAndDepth("RestParticipant " + " endpointURI: " + endpointURI, lra.nestedDepth);
            Response response = sendCompleteOrCompensate(lra, endpointURI, isCancel);
            int responsestatus = response.getStatus(); // expected codes 200, 202, 409, 410
            String readEntity = response.readEntity(String.class);
            logParticipantMessageWithTypeAndDepth("RestParticipant " + lra.getConditionalStringValue(isCancel, "compensate", "complete") +
                            " finished,  response:" + response + ":" + responsestatus + " readEntity:" + readEntity,
                    lra.nestedDepth);
            if (responsestatus == 503) { //  Service Unavailable, retriable - todo this should be the full range of invalid/retriable values
                lra.isRecovering = true;
            } else if (responsestatus == 409) { //conflict, retriable
                lra.isRecovering = true;
            } else if (responsestatus == 202) { //accepted
                lra.isRecovering = true;
            } else if (responsestatus == 404) {
                lra.isRecovering = true;
            } else if (responsestatus == 200 || responsestatus == 410) { // successful or gone (where presumption is complete or compensated)
                setParticipantStatus(isCancel ? Compensated : Completed);
            } else {
                lra.isRecovering = true;
            }
        } catch (Exception e) { // Exception:javax.ws.rs.ProcessingException: java.net.ConnectException: Connection refused (Connection refused)
            logParticipantMessageWithTypeAndDepth("RestParticipant sendCompleteOrCancel Exception:" + e, lra.nestedDepth);
            lra.isRecovering = true;
        }
    }

    private Response sendCompleteOrCompensate(LRA lra, URI endpointURI, boolean isCompensate) {
        logParticipantMessageWithTypeAndDepth("parentId:" + lra.parentId, lra.nestedDepth);
        return client.target(endpointURI)
                .request()
                .header(LRA_HTTP_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                .header(LRA_HTTP_ENDED_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                .header(LRA_HTTP_PARENT_CONTEXT_HEADER, lra.parentId)
                .header(LRA_HTTP_RECOVERY_HEADER, Coordinator.coordinatorURL + lra.lraId)
                .buildPut(Entity.text(lra.getConditionalStringValue(isCompensate, LRAStatus.Cancelled.name(), LRAStatus.Closed.name()))).invoke();
        //                       .buildPut(Entity.json("")).invoke();
        //                       .async().put(Entity.json("entity"));
    }

    public void sendAfterLRA(LRA lra) {
        try {
            URI afterURI = getAfterURI();
            if (afterURI != null) {
                logParticipantMessageWithTypeAndDepth("RestParticipant " + " afterURI: " + afterURI, lra.nestedDepth);
                if (isAfterLRASuccessfullyCalledIfEnlisted()) return;
                Response response = client.target(afterURI)
                        .request()
                        .header(LRA_HTTP_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                        .header(LRA_HTTP_ENDED_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                        .header(LRA_HTTP_PARENT_CONTEXT_HEADER, lra.parentId)
                        .header(LRA_HTTP_RECOVERY_HEADER, Coordinator.coordinatorURL + lra.lraId)
                        .buildPut(Entity.text(lra.getConditionalStringValue(lra.isCancel, LRAStatus.Cancelled.name(), LRAStatus.Closed.name())))
                        .invoke();
                int responsestatus = response.getStatus();
                if (responsestatus == 200) setAfterLRASuccessfullyCalledIfEnlisted();
                logParticipantMessageWithTypeAndDepth("RestParticipant afterLRA finished, response:" + response, lra.nestedDepth);
            }
        } catch (Exception e) {
            logParticipantMessageWithTypeAndDepth("RestParticipant afterLRA Exception:" + e, lra.nestedDepth);
        }
    }


    public void sendStatus(LRA lra, URI statusURI) {
        Response response = null;
        int responsestatus = -1;
        String readEntity = null;
        ParticipantStatus participantStatus = null;
        try {
            response = client.target(statusURI)
                    .request()
                    .header(LRA_HTTP_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .header(LRA_HTTP_ENDED_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .header(LRA_HTTP_PARENT_CONTEXT_HEADER, lra.parentId)
                    .header(LRA_HTTP_RECOVERY_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .buildGet().invoke();
            responsestatus = response.getStatus();
            if (responsestatus == 503 || responsestatus == 202) { //todo include other retriables
            } else if (responsestatus != 410) {
                readEntity = response.readEntity(String.class);
                participantStatus = ParticipantStatus.valueOf(readEntity);
                setParticipantStatus(participantStatus);
            } else {
                setParticipantStatus(lra.isCancel ? Compensated : Completed); // not exactly accurate as it's GONE not explicitly completed or compensated
            }
            logParticipantMessageWithTypeAndDepth("LRA sendStatus:" + statusURI + " finished  response:" +
                    response + ":" + responsestatus + " participantStatus:" + participantStatus +
                    " readEntity:" + readEntity, lra.nestedDepth);
        } catch (Exception e) { // IllegalArgumentException: No enum constant org.eclipse.microprofile.lra.annotation.ParticipantStatus.
            logParticipantMessageWithTypeAndDepth("LRA sendStatus:" + statusURI + " finished  response:" +
                    response + ":" + responsestatus + " participantStatus:" + participantStatus +
                    " readEntity:" + readEntity + " Exception:" + e, lra.nestedDepth);
        }
    }

    public boolean sendForget(LRA lra) {
        boolean isForgotten = true;
        try {
            Response response = client.target(getForgetURI())
                    .request()
                    .header(LRA_HTTP_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .header(LRA_HTTP_ENDED_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .header(LRA_HTTP_PARENT_CONTEXT_HEADER, lra.parentId)
                    .header(LRA_HTTP_RECOVERY_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .buildDelete().invoke();
            int responsestatus = response.getStatus();
            logParticipantMessageWithTypeAndDepth("RestParticipant sendForget:" + getForgetURI() + " finished  response:" + response + ":" + responsestatus, lra.nestedDepth);
            if (responsestatus == 200 || responsestatus == 410) {
                setForgotten();
            } else {
                isForgotten = false;
            }
        } catch (Exception e) {
            logParticipantMessageWithTypeAndDepth("RestParticipant sendForget Exception:" + e, lra.nestedDepth);
            isForgotten = false;
        }
        return isForgotten;
    }

}
