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
package io.helidon.lra;

import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import java.net.URI;

import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.*;
import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;

public abstract class Participant {

    /**
     *   Participant state model....
     *   Active -------------------------------------------------------------------------> Compensating --> FailedToCompensate
     *          --> Completing --> FailedToComplete                                       /             --> Compensated
     *                         --> Completed --> (only if nested can go to compensating) /
     */
    private boolean isAfterLRASuccessfullyCalledIfEnlisted;
    private boolean isForgotten;
    private ParticipantStatus participantStatus;
    private URI completeURI; // a method to be executed when the LRA is closed 200, 202, 409, 410
    private URI compensateURI; //  a method to be executed when the LRA is cancelled 200, 202, 409, 410
    private URI afterURI;  // a method that will be reliably invoked when the LRA enters one of the final states 200
    private URI forgetURI; // a method to be executed when the LRA allows participant to clear all associated information 200, 410
    private URI statusURI; // a method that allow user to state status of the participant with regards to a particular LRA 200, 202, 410

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
        return "ParticipantStatus:" + participantStatus +
                " completeURI:" + completeURI +
                " compensateURI:" + compensateURI +
                " afterURI:" + afterURI +
                " forgetURI:" + forgetURI +
                " statusURI:" + statusURI;
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


    public void log(String message, int nestedDepth) {
        System.out.println("[lra][depth:" + nestedDepth + "] " + message);
    }

    abstract boolean sendForget(LRA lra, boolean areAllThatNeedToBeForgottenForgotten);

    abstract void sendAfterLRA(LRA lra);

    abstract void sendCompleteOrCancel(LRA lra, boolean isCancel);

    abstract void sendStatus(LRA lra, URI statusURI);
}
