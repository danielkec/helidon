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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlID;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlRootElement;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_ENDED_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class LRA {

    private long timeout;
    @XmlID
    public String lraId;
    public URI parentId;
    Set<String> compensatorLinks = new HashSet<>();
    @XmlIDREF
    LRA parent;
    @XmlIDREF
    List<LRA> children = new ArrayList<>();
    @XmlElement
    List<Participant> participants = new ArrayList<>();

    private AtomicReference<LRAStatus> status = new AtomicReference<>(LRAStatus.Active);

    public boolean isRecovering = false;
    public boolean isCancel;
    boolean isRoot = false;
    boolean isParent;
    boolean isChild;
    boolean isNestedThatShouldBeForgottenAfterParentEnds = false;
    private boolean isProcessing;
    private long whenReadyToDelete = 0;

    public LRA(String lraUUID) {
        lraId = lraUUID;
        isRoot = true;
    }

    public LRA(String lraUUID, URI parentId) {
        lraId = lraUUID;
        this.parentId = parentId;
    }

    public LRA() {
    }

    public void setupTimeout(long timeLimit) {
        if (timeLimit != 0) {
            this.timeout = System.currentTimeMillis() + timeLimit;
        } else {
            this.timeout = 0;
        }
    }

    public boolean checkTimeout() {
        return timeout > 0 && timeout < System.currentTimeMillis();
    }

    /**
     * Parse the compensatorLink received from join call from participant and add participant (whether it be REST, AQ, or Kafka)
     *
     * @param compensatorLink from REST or message header/property/value
     * @return debug string
     */
    public void addParticipant(String compensatorLink) {
        if (compensatorLinks.add(compensatorLink)) {
            Participant participant = new Participant();
            participant.parseCompensatorLinks(compensatorLink);
            participants.add(participant);
        }
    }

    /**
     * Remove participant that has asked to leave
     *
     * @param compensatorUrl
     */
    public void removeParticipant(String compensatorUrl) {
        Set<Participant> forRemove = participants.stream()
                .filter(p -> p.equalCompensatorUris(compensatorUrl))
                .collect(Collectors.toSet());
        forRemove.forEach(participants::remove);
    }

    /**
     * Add child in nested LRA, mark it as a child, set it's parent, and mark parent as parent
     *
     * @param lra The child
     */
    public void addChild(LRA lra) {
        children.add(lra);
        lra.isChild = true;
        lra.parent = this;
        isParent = true;
    }

    public MultivaluedMap<String, Object> headers() {
        //String coordinatorLraUrl = Coordinator.coordinatorURL + lraId;
        MultivaluedMap<String, Object> multivaluedMap = new MultivaluedHashMap<>(4);
        multivaluedMap.add(LRA_HTTP_CONTEXT_HEADER, lraId);
        multivaluedMap.add(LRA_HTTP_ENDED_CONTEXT_HEADER, lraId);
        multivaluedMap.add(LRA_HTTP_PARENT_CONTEXT_HEADER, parentId);
        multivaluedMap.add(LRA_HTTP_RECOVERY_HEADER, lraId);
        return multivaluedMap;
    }

    void terminate(boolean isCancel, boolean isUnilateralCallIfNested) {
        setProcessing(true);
        status.updateAndGet(unused -> isCancel ? LRAStatus.Cancelling : LRAStatus.Closing);
        this.isCancel = isCancel;
        if (isUnilateralCallIfNested && isChild && !isCancel) isNestedThatShouldBeForgottenAfterParentEnds = true;
        if (isChild && !isCancel && areAllInEndStateCompensatedOrFailedToCompensate()) {
            return; //todo this only checks this child, not children of this child, verify correctness
        }
        if (isParent) {
            for (LRA nestedLRA : children) {
                if (!nestedLRA.areAllInEndStateOrListenerOnlyForTerminationType(isCancel)) {
                    nestedLRA.terminate(isCancel, false); //todo this is the classic afterLRA/tx sync scenario - need to check if we traverse the tree twice or couple end and listener calls
                }
            }
        }
        sendCompleteOrCancel(isCancel);
        sendAfterLRA();
        if (areAllInEndState() && areAllAfterLRASuccessfullyCalledOrForgotten()) {
            if (forgetAnyUnilaterallyCompleted()) {
                status.updateAndGet(unused -> isCancel ? LRAStatus.Cancelled : LRAStatus.Closed);
                // keep terminated for 5 minutes before deletion
                whenReadyToDelete = System.currentTimeMillis() + 5 * 1000 * 60;
            }
        }
        setProcessing(false);
    }

    public boolean forgetAnyUnilaterallyCompleted() {
        for (LRA nestedLRA : children) {
            if (nestedLRA.isNestedThatShouldBeForgottenAfterParentEnds) {
                if (!nestedLRA.sendForget()) return false;
            }
        }
        return true;
    }
    public AtomicReference<LRAStatus> status() {
        return status;
    }

    private void sendCompleteOrCancel(boolean isCancel) {
        for (Participant participant : participants) {
            if (participant.isInEndStateOrListenerOnly() && !isChild) { //todo check ramifications of !isChild re timeout processing
                continue;
            }
            participant.sendCompleteOrCancel(this, isCancel);
        }
    }


    void sendAfterLRA() { // todo should set isRecovering or needsAfterLRA calls if this fails
        if (areAllInEndState()) {
            for (Participant participant : participants) {
                participant.sendAfterLRA(this);
            }
        }
    }

    void trySendStatus() {
        if (!hasStatusEndpoints()) {
            return;
        }
        for (Participant participant : participants) {
            Optional<URI> statusURI = participant.getStatusURI();
            if (statusURI.isEmpty() || participant.isInEndStateOrListenerOnly()) continue;
            participant.sendStatus(this, statusURI.get());
        }
    }

    boolean sendForget() { //todo could gate with isprocessing here as well
        boolean areAllThatNeedToBeForgotten = true;
        for (Participant participant : participants) {
            if (participant.getForgetURI().isEmpty() || participant.isForgotten()) continue;
            areAllThatNeedToBeForgotten = participant.sendForget(this);
        }
        return areAllThatNeedToBeForgotten;
    }

    public void setProcessing(boolean isProcessing) {
        this.isProcessing = isProcessing;
    }

    public boolean isProcessing() {
        return isProcessing;
    }

    public boolean isReadyToDelete() {
        return whenReadyToDelete != 0 && whenReadyToDelete < System.currentTimeMillis();
    }

    public boolean hasStatusEndpoints() {
        return participants.stream()
                .map(Participant::getStatusURI)
                .anyMatch(Optional::isPresent);
    }

    public boolean areAnyInFailedState() {
        for (Participant participant : participants) {
            if (participant.getParticipantStatus() == ParticipantStatus.FailedToComplete ||
                    participant.getParticipantStatus() == ParticipantStatus.FailedToCompensate) {
                return true;
            }
        }
        return false;
    }

    public boolean areAllAfterLRASuccessfullyCalledOrForgotten() {
        for (Participant participant : participants) {
            if (!participant.isAfterLRASuccessfullyCalledIfEnlisted() && !participant.isForgotten()) return false;
        }
        return true;
    }

    public boolean areAllInEndState() {
        for (Participant participant : participants) {
            if (!participant.isInEndStateOrListenerOnly()) return false;
        }
        return true;
    }

    public boolean areAllInEndStateCompensatedOrFailedToCompensate() {
        for (Participant participant : participants) {
            if (participant.getParticipantStatus() != ParticipantStatus.Compensated && participant.getParticipantStatus() != ParticipantStatus.FailedToCompensate) {
                return false;
            }
        }
        return true;
    }

    public boolean areAllInEndStateOrListenerOnlyForTerminationType(boolean isCompensate) {
        for (Participant participant : participants) {
            if (!participant.isInEndStateOrListenerOnlyForTerminationType(isCompensate)) return false;
        }
        return true;
    }
}
