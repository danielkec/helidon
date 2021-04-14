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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

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
//    private URI completeURI; // a method to be executed when the LRA is closed 200, 202, 409, 410
//    private URI compensateURI; // a method to be executed when the LRA is cancelled 200, 202, 409, 410
//    private URI afterURI;  // a method that will be reliably invoked when the LRA enters one of the final states 200
//    private URI forgetURI; // a method to be executed when the LRA allows participant to clear all associated information 200, 410
//    private URI statusURI; // a method that allow user to state status of the participant with regards to a particular LRA 200, 202, 410

    @XmlElement
    @XmlJavaTypeAdapter(Link.JaxbAdapter.class)
    private final List<Link> compensatorLinks = new ArrayList<>(5);
    
    @XmlTransient
    private final Client client = ClientBuilder.newBuilder().build();

    void parseCompensatorLinks(String compensatorLinks){
        Stream.of(compensatorLinks.split(","))
                .map(Link::valueOf)
                .forEach(this.compensatorLinks::add);
    }
    
    ParticipantStatus getParticipantStatus() {
        return participantStatus;
    }

    //todo handle out of order messages, ie validate against state machine
    public void setParticipantStatus(ParticipantStatus participantStatus) {
        this.participantStatus = participantStatus;
    }

    public Optional<Link> getCompensatorLink(String rel){
        return compensatorLinks.stream().filter(l -> rel.equals(l.getRel())).findFirst();
    }
    
    public Optional<URI> getCompleteURI() {
        return getCompensatorLink("complete").map(Link::getUri);
    }

    public Optional<URI> getCompensateURI() {
        return getCompensatorLink("compensate").map(Link::getUri);
    }

    public Optional<URI> getAfterURI() {
        return getCompensatorLink("after").map(Link::getUri);
    }

    public Optional<URI> getForgetURI() {
        return getCompensatorLink("forget").map(Link::getUri);
    }

    public Optional<URI> getStatusURI() {
        return getCompensatorLink("status").map(Link::getUri);
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
        return isAfterLRASuccessfullyCalledIfEnlisted || getAfterURI().isEmpty();
    }

    //A listener is a participant with afterURI endpoint. It may not have a complete or compensate endpoint.
    boolean isListenerOnly() {
        return getCompleteURI().isEmpty() && getCompensateURI().isEmpty();
    }

    public String toString() {
        return "ParticipantStatus:" + participantStatus +
                "\n completeURI:" + getCompleteURI() +
                "\n compensateURI:" + getCompensateURI() +
                "\n afterURI:" + getAfterURI() +
                "\n forgetURI:" + getForgetURI() +
                "\n statusURI:" + getStatusURI();
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
    
    void sendCompleteOrCancel(LRA lra, boolean isCancel) {
        Optional<URI> endpointURI = isCancel ? getCompensateURI() : getCompleteURI();
        try {
            Response response = sendCompleteOrCompensate(lra, endpointURI.get(), isCancel);
            int responsestatus = response.getStatus(); // expected codes 200, 202, 409, 410
            if (responsestatus == 200 || responsestatus == 410) { // successful or gone (where presumption is complete or compensated)
                setParticipantStatus(isCancel ? Compensated : Completed);
            } else {
                lra.isRecovering = true;
            }
        } catch (Exception e) {
            lra.isRecovering = true;
            LOGGER.log(Level.SEVERE, "Error when completing/canceling", e);
        }
    }

    private Response sendCompleteOrCompensate(LRA lra, URI endpointURI, boolean isCompensate) {
        return client.target(endpointURI)
                .request()
                .header(LRA_HTTP_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                .header(LRA_HTTP_ENDED_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                .header(LRA_HTTP_PARENT_CONTEXT_HEADER, lra.parentId)
                .header(LRA_HTTP_RECOVERY_HEADER, Coordinator.coordinatorURL + lra.lraId)
                .buildPut(Entity.text(isCompensate ? LRAStatus.Cancelled.name() : LRAStatus.Closed.name()))
                .invoke();
    }

    public void sendAfterLRA(LRA lra) {
        try {
            Optional<URI> afterURI = getAfterURI();
            if (afterURI.isPresent()) {
                if (isAfterLRASuccessfullyCalledIfEnlisted()) return;
                Response response = client.target(afterURI.get())
                        .request()
                        .header(LRA_HTTP_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                        .header(LRA_HTTP_ENDED_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                        .header(LRA_HTTP_PARENT_CONTEXT_HEADER, lra.parentId)
                        .header(LRA_HTTP_RECOVERY_HEADER, Coordinator.coordinatorURL + lra.lraId)
                        .buildPut(Entity.text(lra.isCancel ? LRAStatus.Cancelled.name() : LRAStatus.Closed.name()))
                        .invoke();
                int responseStatus = response.getStatus();
                if (responseStatus == 200) setAfterLRASuccessfullyCalledIfEnlisted();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error when sending after lra", e);
        }
    }


    public void sendStatus(LRA lra, URI statusURI) {
        Response response;
        int responseStatus;
        String readEntity;
        ParticipantStatus participantStatus;
        try {
            response = client.target(statusURI)
                    .request()
                    .header(LRA_HTTP_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .header(LRA_HTTP_ENDED_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .header(LRA_HTTP_PARENT_CONTEXT_HEADER, lra.parentId)
                    .header(LRA_HTTP_RECOVERY_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .buildGet().invoke();
            responseStatus = response.getStatus();
            if (responseStatus == 503 || responseStatus == 202) { //todo include other retriables
            } else if (responseStatus != 410) {
                readEntity = response.readEntity(String.class);
                participantStatus = ParticipantStatus.valueOf(readEntity);
                setParticipantStatus(participantStatus);
            } else {
                setParticipantStatus(lra.isCancel ? Compensated : Completed); // not exactly accurate as it's GONE not explicitly completed or compensated
            }
        } catch (Exception e) { // IllegalArgumentException: No enum constant org.eclipse.microprofile.lra.annotation.ParticipantStatus.
            e.printStackTrace();
        }
    }

    public boolean sendForget(LRA lra) {
        boolean isForgotten = true;
        try {
            Response response = client.target(getForgetURI().get())
                    .request()
                    .header(LRA_HTTP_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .header(LRA_HTTP_ENDED_CONTEXT_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .header(LRA_HTTP_PARENT_CONTEXT_HEADER, lra.parentId)
                    .header(LRA_HTTP_RECOVERY_HEADER, Coordinator.coordinatorURL + lra.lraId)
                    .buildDelete().invoke();
            int responsestatus = response.getStatus();
            if (responsestatus == 200 || responsestatus == 410) {
                setForgotten();
            } else {
                isForgotten = false;
            }
        } catch (Exception e) {
            isForgotten = false;
        }
        return isForgotten;
    }

    boolean equalCompensatorUris(String compensatorUris) {
        Set<Link> links = Arrays.stream(compensatorUris.split(","))
                .map(Link::valueOf)
                .collect(Collectors.toSet());

        for (Link link : links) {
            Optional<Link> participantsLink = getCompensatorLink(link.getRel());
            if(participantsLink.isEmpty()){
                continue;
            }
            
            if(Objects.equals(participantsLink.get().getUri(), link.getUri())){
                return true;
            }
        }

        return false;
    }

}
