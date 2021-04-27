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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
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

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class LRA {

    private static final Logger LOGGER = Logger.getLogger(LRA.class.getName());

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
        MultivaluedMap<String, Object> multivaluedMap = new MultivaluedHashMap<>(4);
        multivaluedMap.add(LRA_HTTP_CONTEXT_HEADER, lraId);
        multivaluedMap.add(LRA_HTTP_ENDED_CONTEXT_HEADER, lraId);
        multivaluedMap.add(LRA_HTTP_PARENT_CONTEXT_HEADER, parentId);
        multivaluedMap.add(LRA_HTTP_RECOVERY_HEADER, lraId);
        return multivaluedMap;
    }

    void close() {
        Set<LRAStatus> allowedStatuses = Set.of(LRAStatus.Active, LRAStatus.Closing);
        if (LRAStatus.Closing != status.updateAndGet(old -> allowedStatuses.contains(old) ? LRAStatus.Closing : old)) {
            LOGGER.warning("Can't close LRA, it's already " + status.get().name() + " " + this.lraId);
            return;
        }
        for (LRA nestedLRA : children) {
            nestedLRA.close();
        }
        sendComplete();
        sendAfterLRA();
    }

    void cancel() {
        Set<LRAStatus> allowedStatuses = Set.of(LRAStatus.Active, LRAStatus.Cancelling);
        if (LRAStatus.Cancelling != status.updateAndGet(old -> allowedStatuses.contains(old) ? LRAStatus.Cancelling : old)) {
            LOGGER.warning("Can't cancel LRA, it's already " + status.get().name() + " " + this.lraId);
            return;
        }
        for (LRA nestedLRA : children) {
            nestedLRA.cancel();
        }
        sendCancel();
        sendAfterLRA();
    }

    void terminate() {
        for (LRA nestedLRA : children) {
            if (!nestedLRA.areAllInEndStateOrListenerOnlyForTerminationType(isCancel)) {
                nestedLRA.terminate();
            }
        }
        cancel();
        if (!sendAfterLRA()) return; // not all afters sent
        if (areAllInEndState() && areAllAfterLRASuccessfullyCalledOrForgotten()) {
            if (forgetAnyUnilaterallyCompleted()) {
                // keep terminated for 5 minutes before deletion
                whenReadyToDelete = System.currentTimeMillis() + 5 * 1000 * 60;
            }
        }
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

    private void sendComplete() {
        boolean allClosed = true;
        for (Participant participant : participants) {
            if (participant.isInEndStateOrListenerOnly() && !isChild) {
                continue;
            }
            allClosed = allClosed && participant.sendComplete(this);
        }
        if (allClosed) {
            this.status().compareAndSet(LRAStatus.Closing, LRAStatus.Closed);
        }
    }

    private void sendCancel() {
        boolean allClosed = true;
        for (Participant participant : participants) {
            if (participant.isInEndStateOrListenerOnly() && !isChild) {
                continue;
            }
            allClosed = allClosed && participant.sendCancel(this);
        }
        if (allClosed) {
            this.status().compareAndSet(LRAStatus.Cancelling, LRAStatus.Cancelled);
        }
    }


    boolean sendAfterLRA() {
        if (!areAllInEndState()) {
            return false;
        }

        boolean allSent = true;
        for (Participant participant : participants) {
            allSent = allSent && participant.sendAfterLRA(this);
        }
        return allSent;
    }

    boolean sendForget() { //todo could gate with isprocessing here as well
        boolean areAllThatNeedToBeForgotten = true;
        for (Participant participant : participants) {
            if (participant.getForgetURI().isEmpty() || participant.isForgotten()) continue;
            areAllThatNeedToBeForgotten = participant.sendForget(this);
        }
        return areAllThatNeedToBeForgotten;
    }

    public boolean isReadyToDelete() {
        return whenReadyToDelete != 0 && whenReadyToDelete < System.currentTimeMillis();
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

    public boolean areAllInEndStateOrListenerOnlyForTerminationType(boolean isCompensate) {
        for (Participant participant : participants) {
            if (!participant.isInEndStateOrListenerOnlyForTerminationType(isCompensate)) return false;
        }
        return true;
    }
}
