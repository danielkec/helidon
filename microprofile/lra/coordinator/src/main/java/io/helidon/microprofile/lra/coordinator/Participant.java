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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final AtomicReference<AfterLraStatus> afterLRACalled = new AtomicReference<>(AfterLraStatus.NOT_SENT);
    private final AtomicReference<ParticipantStatus> participantStatus = new AtomicReference<>(ParticipantStatus.Active);
    private final AtomicReference<SendingStatus> sendingStatus = new AtomicReference<>(SendingStatus.NOT_SENDING);
    AtomicInteger remainingCloseAttempts = new AtomicInteger(5);
    AtomicInteger remainingAfterLraAttempts = new AtomicInteger(5);

    //TODO: Participant needs custom states
    enum SendingStatus {
        SENDING, NOT_SENDING;
    }

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
        return participantStatus.get();
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
        return isListenerOnly()
                || Set.of(ParticipantStatus.FailedToComplete,
                ParticipantStatus.FailedToCompensate,
                ParticipantStatus.Completed,
                ParticipantStatus.Compensated)
                .contains(this.participantStatus.get());
    }

    public boolean isInEndStateOrListenerOnlyForTerminationType(boolean isCompensate) {
        ParticipantStatus status = this.participantStatus.get();
        if (isCompensate) {
            return status == ParticipantStatus.FailedToCompensate ||
                    status == ParticipantStatus.Compensated ||
                    isListenerOnly();
        } else {
            return status == ParticipantStatus.FailedToComplete ||
                    status == ParticipantStatus.Completed ||
                    isListenerOnly();
        }
    }

    boolean sendCancel(LRA lra) {
        if (!sendingStatus.compareAndSet(SendingStatus.NOT_SENDING, SendingStatus.SENDING)) return false;
        Optional<URI> endpointURI = getCompensateURI();
        try {
            Response response = client.target(endpointURI.get())
                    .request()
                    .headers(lra.headers())
                    .async()
                    .put(Entity.text(LRAStatus.Cancelled.name()))
                    .get(500, TimeUnit.MILLISECONDS);

            switch (response.getStatus()) {
                // complete or compensated
                case 200:
                case 202:
                case 410:
                    LOGGER.log(Level.INFO, "Compensated participant of LRA {0} {1}", new Object[] {lra.lraId, this.getCompensateURI()});
                    participantStatus.set(ParticipantStatus.Compensated);
                    return true;

                // retryable
                case 409:
                case 404:
                case 503:
                default:
                    throw new Exception(response.getStatusInfo() + " " + response.getStatusInfo().getReasonPhrase());
            }

        } catch (Exception e) {
            if (remainingCloseAttempts.decrementAndGet() <= 0) {
                LOGGER.log(Level.WARNING, "Failed to compensate participant of LRA {0} {1} {2}",
                        new Object[] {lra.lraId, this.getCompensateURI(), e.getMessage()});
                participantStatus.set(ParticipantStatus.FailedToCompensate);
            }
            // If the participant does not support idempotency then it MUST be able to report its status 
            // by annotating one of the methods with the @Status annotation which should report the status
            // in case we can't retrieve status from participant just retry n times
            retrieveStatus(lra).ifPresent(participantStatus::set);
        } finally {
            sendingStatus.set(SendingStatus.NOT_SENDING);
        }
        return false;
    }

    boolean sendComplete(LRA lra) {
        if (!sendingStatus.compareAndSet(SendingStatus.NOT_SENDING, SendingStatus.SENDING)) return false;
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
                    participantStatus.set(ParticipantStatus.Completed);
                    return true;

                // retryable
                case 409:
                case 404:
                case 503:
                default:
                    throw new Exception(response.getStatusInfo() + " " + response.getStatusInfo().getReasonPhrase());
            }

        } catch (Exception e) {
            if (remainingCloseAttempts.decrementAndGet() <= 0) {
                LOGGER.log(Level.WARNING, "Failed to complete participant of LRA {0} {1} {2}",
                        new Object[] {lra.lraId, this.getCompleteURI(), e.getMessage()});
                participantStatus.set(ParticipantStatus.FailedToComplete);
            }
            // If the participant does not support idempotency then it MUST be able to report its status 
            // by annotating one of the methods with the @Status annotation which should report the status
            // in case we can't retrieve status from participant just retry n times
            retrieveStatus(lra).ifPresent(participantStatus::set);
        } finally {
            sendingStatus.set(SendingStatus.NOT_SENDING);
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
                if (status == 200) {
                    afterLRACalled.set(AfterLraStatus.SENT);
                } else if (remainingAfterLraAttempts.decrementAndGet() <= 0) {
                    afterLRACalled.set(AfterLraStatus.SENT);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error when sending after lra", e);
        }
        return afterLRACalled.get() == AfterLraStatus.SENT;
    }


    public Optional<ParticipantStatus> retrieveStatus(LRA lra) {
        Optional<URI> statusURI = this.getStatusURI();
        if (statusURI.isPresent()) {
            try {
                Response response = client.target(statusURI.get())
                        .request()
                        .headers(lra.headers())
                        .buildGet().invoke();
                int responseStatus = response.getStatus();
                if (responseStatus == 503 || responseStatus == 202) { //todo include other retriables
                } else if (responseStatus != 410) {
                    return Optional.of(ParticipantStatus.valueOf(response.readEntity(String.class)));
                }
            } catch (Exception e) { // IllegalArgumentException: No enum constant org.eclipse.microprofile.lra.annotation.ParticipantStatus.
                LOGGER.log(Level.SEVERE, "Error when getting participant status. " + statusURI, e);
            }
        }
        return Optional.empty();
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
