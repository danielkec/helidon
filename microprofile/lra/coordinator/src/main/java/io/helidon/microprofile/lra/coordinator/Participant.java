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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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

import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Participant {

    private static final Logger LOGGER = Logger.getLogger(Participant.class.getName());
    private boolean isAfterLRASuccessfullyCalledIfEnlisted;
    private boolean isForgotten;
    private AtomicReference<AfterLraStatus> afterLRACalled = new AtomicReference<>(AfterLraStatus.NOT_SENT);
    private ParticipantStatus participantStatus;

    enum AfterLraStatus {
        NOT_SENT, SENDING, SENT;
    }

    @XmlElement
    @XmlJavaTypeAdapter(Link.JaxbAdapter.class)
    private final List<Link> compensatorLinks = new ArrayList<>(5);

    @XmlTransient
    private final Client client = ClientBuilder.newBuilder().build();

    void parseCompensatorLinks(String compensatorLinks) {
        Stream.of(compensatorLinks.split(","))
                .filter(s -> !s.isBlank())
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


    public Optional<Link> getCompensatorLink(String rel) {
        return compensatorLinks.stream().filter(l -> rel.equals(l.getRel())).findFirst();
    }

    /**
     * Invoked when closed 200, 202, 409, 410
     */
    public Optional<URI> getCompleteURI() {
        return getCompensatorLink("complete").map(Link::getUri);
    }

    /**
     * Invoked when cancelled 200, 202, 409, 410
     */
    public Optional<URI> getCompensateURI() {
        return getCompensatorLink("compensate").map(Link::getUri);
    }

    /**
     * Invoked when finalized 200
     */
    public Optional<URI> getAfterURI() {
        return getCompensatorLink("after").map(Link::getUri);
    }

    /**
     * Invoked when cleaning up 200, 410
     */
    public Optional<URI> getForgetURI() {
        return getCompensatorLink("forget").map(Link::getUri);
    }

    /**
     * Directly updates status of participant 200, 202, 410
     */
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

    public boolean isInEndStateOrListenerOnly() {
        return participantStatus == ParticipantStatus.FailedToComplete ||
                participantStatus == ParticipantStatus.FailedToCompensate ||
                participantStatus == ParticipantStatus.Completed ||
                participantStatus == ParticipantStatus.Compensated ||
                isListenerOnly();
    }

    public boolean isInEndStateOrListenerOnlyForTerminationType(boolean isCompensate) {
        if (isCompensate) {
            return participantStatus == ParticipantStatus.FailedToCompensate ||
                    participantStatus == ParticipantStatus.Compensated ||
                    isListenerOnly();
        } else {
            return participantStatus == ParticipantStatus.FailedToComplete ||
                    participantStatus == ParticipantStatus.Completed ||
                    isListenerOnly();
        }
    }

    boolean sendCancel(LRA lra) {
        Optional<URI> endpointURI = getCompensateURI();
        try {
            Response response = client.target(endpointURI.get())
                    .request()
                    .headers(lra.headers())
                    .buildPut(Entity.text(LRAStatus.Cancelled.name()))
                    .invoke();

            switch (response.getStatus()) {
                // complete or compensated
                case 200:                
                case 202:
                case 410:
                    setParticipantStatus(ParticipantStatus.Compensated);
                    return true;

                // retryable
                case 409:
                case 404:
                case 503:
                default:
                    setParticipantStatus(ParticipantStatus.FailedToCompensate);
            }

        } catch (Exception e) {
            setParticipantStatus(ParticipantStatus.FailedToCompensate);
            LOGGER.log(Level.SEVERE, "Error when completing/canceling", e);
        }
        return false;
    }

    boolean sendComplete(LRA lra) {
        Optional<URI> endpointURI = getCompleteURI();
        try {
            Response response = client.target(endpointURI.get())
                    .request()
                    .headers(lra.headers())
                    .buildPut(Entity.text(LRAStatus.Closed.name()))
                    .invoke();

            switch (response.getStatus()) {
                // complete or compensated
                case 200:
                case 202:
                case 410:
                    setParticipantStatus(ParticipantStatus.Completed);
                    return true;

                // retryable
                case 409:
                case 404:
                case 503:
                default:
                    setParticipantStatus(ParticipantStatus.FailedToComplete);
            }

        } catch (Exception e) {
            setParticipantStatus(ParticipantStatus.FailedToComplete);
            LOGGER.log(Level.SEVERE, "Error when completing/canceling", e);
        }
        return false;
    }

    public boolean sendAfterLRA(LRA lra) {
        try {
            Optional<URI> afterURI = getAfterURI();
            if (afterURI.isPresent() && afterLRACalled.compareAndSet(AfterLraStatus.NOT_SENT, AfterLraStatus.SENDING)) {
                Response response = client.target(afterURI.get())
                        .request()
                        .headers(lra.headers())
                        .buildPut(Entity.text(lra.status().get().name()))
                        .invoke();
                int status = response.getStatus();
                afterLRACalled.updateAndGet(old -> status == 200 ? AfterLraStatus.SENT : AfterLraStatus.NOT_SENT);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error when sending after lra", e);
        }
        return afterLRACalled.get() == AfterLraStatus.SENT;
    }


    public void retrieveStatus(LRA lra, URI statusURI) {
        Response response;
        int responseStatus;
        String readEntity;
        ParticipantStatus participantStatus;
        try {
            response = client.target(statusURI)
                    .request()
                    .headers(lra.headers())
                    .buildGet().invoke();
            responseStatus = response.getStatus();
            if (responseStatus == 503 || responseStatus == 202) { //todo include other retriables
            } else if (responseStatus != 410) {
                readEntity = response.readEntity(String.class);
                participantStatus = ParticipantStatus.valueOf(readEntity);
                setParticipantStatus(participantStatus);
            } else {
                setParticipantStatus(lra.isCancel ? ParticipantStatus.Compensated : ParticipantStatus.Completed); // not exactly accurate as it's GONE not explicitly completed or compensated
            }
        } catch (Exception e) { // IllegalArgumentException: No enum constant org.eclipse.microprofile.lra.annotation.ParticipantStatus.
            LOGGER.log(Level.SEVERE, "Error when sending status.", e);
        }
    }

    public boolean sendForget(LRA lra) {
        boolean isForgotten = true;
        try {
            Response response = client.target(getForgetURI().get())
                    .request()
                    .headers(lra.headers())
                    .buildDelete().invoke();
            int responseStatus = response.getStatus();
            if (responseStatus == 200 || responseStatus == 410) {
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
            if (participantsLink.isEmpty()) {
                continue;
            }

            if (Objects.equals(participantsLink.get().getUri(), link.getUri())) {
                return true;
            }
        }

        return false;
    }

}