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
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlID;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlRootElement;

import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;
import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.Completed;
import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.FailedToCompensate;
import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.FailedToComplete;

import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class LRA {

    /**
     * LRA state model....
     * Active -----------------------------------------------------------------> Cancelling --> FailedToCancel
     * --> Closing --> FailedToClose                                     /                  --> Cancelled
     * --> Closed --> (only if nested can go to cancelling) /
     */
    private static final Logger LOGGER = Logger.getLogger(LRA.class.getName());
    private long timeout;
    @XmlID
    public String lraId;
    public URI parentId;
    List<String> compensatorLinks = new ArrayList<>();
    @XmlIDREF
    LRA parent;
    @XmlIDREF
    List<LRA> children = new ArrayList<>();
    @XmlElement
    List<Participant> participants = new ArrayList<>();
    boolean hasStatusEndpoints; //todo remove as this is check at participant level

    public boolean isRecovering = false;
    public boolean isCancel;
    boolean isRoot = false;
    boolean isParent;
    boolean isChild;
    boolean isNestedThatShouldBeForgottenAfterParentEnds = false;
    public int nestedDepth;
    private boolean isProcessing;
    private long isReadyToDelete = 0;

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
    String addParticipant(String compensatorLink) {
        if (compensatorLinks.contains(compensatorLink)) {
            return "participant already enlisted"; //todo this should be correct/sufficient but need to test
        } else {
            compensatorLinks.add(compensatorLink);
        }
        Participant participant = new Participant();
        String uriPrefix = "<http://";
        participants.add(participant);
        String endpoint = "";
        Pattern linkRelPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"|([^\\s]+)");
        Matcher relMatcher = linkRelPattern.matcher(compensatorLink);
        while (relMatcher.find()) {
            String group0 = relMatcher.group(0);
            if (group0.contains(uriPrefix)) {
                endpoint = group0.substring(group0.indexOf(uriPrefix) + 1, group0.indexOf(";") - 1);
            }
            String key = relMatcher.group(1);
            if (key != null && key.equals("rel")) {
                String rel = getConditionalStringValue(relMatcher.group(2) == null, relMatcher.group(3), relMatcher.group(2));
                try {
                    if (rel.equals("complete")) {
                        participant.setCompleteURI(new URI(endpoint));
                    }
                    if (rel.equals("compensate")) {
                        participant.setCompensateURI(new URI(endpoint));
                    }
                    if (rel.equals("after")) {
                        participant.setAfterURI(new URI(endpoint));
                    }
                    if (rel.equals("status")) {
                        participant.setStatusURI(new URI(endpoint));
                        hasStatusEndpoints = true;
                    }
                    if (rel.equals("forget")) {
                        participant.setForgetURI(new URI(endpoint));
                    }
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
            }
        }
        if (!participant.init()) {
            throw new RuntimeException("unable to initialize participant:" + participant); //todo better exception/handling
        }
        return "LRA joined/added:" + (getConditionalStringValue(participant.isListenerOnly(), "listener:", "participant:")) + participant;

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

    void terminate(boolean isCancel, boolean isUnilateralCallIfNested) {
        setProcessing(true);
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
                // keep terminated for 5 minutes before deletion
                isReadyToDelete = System.currentTimeMillis() + 5 * 1000 * 60;
            }
        }
        setProcessing(false);
    }

    public boolean forgetAnyUnilaterallyCompleted() {
        boolean isAllThatNeedsToBeForgotten = true;
        for (LRA nestedLRA : children) {
            if (nestedLRA.isNestedThatShouldBeForgottenAfterParentEnds) {
                if (!nestedLRA.sendForget()) return false;
            }
        }
        return isAllThatNeedsToBeForgotten;
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
        if(!hasStatusEndpoints()){
            return;
        }
        for (Participant participant : participants) {
            URI statusURI = participant.getStatusURI();
            if (statusURI == null || participant.isInEndStateOrListenerOnly()) continue;
            participant.sendStatus(this, statusURI);
        }
    }

    boolean sendForget() { //todo could gate with isprocessing here as well
        boolean areAllThatNeedToBeForgottenForgotten = true;
        for (Participant participant : participants) {
            if (participant.getForgetURI() == null || participant.isForgotten()) continue;
            areAllThatNeedToBeForgottenForgotten = participant.sendForget(this);
        }
        return areAllThatNeedToBeForgottenForgotten;
    }

    public void setProcessing(boolean isProcessing) {
        this.isProcessing = isProcessing;
    }

    public boolean isProcessing() {
        return isProcessing;
    }

    public boolean isReadyToDelete() {
        return isReadyToDelete != 0 && isReadyToDelete < System.currentTimeMillis();
    }

    public boolean hasStatusEndpoints() {
        return hasStatusEndpoints;
    }

    public String toString() {
        StringBuilder participantsString = new StringBuilder();
        for (Participant participant : participants) participantsString.append(participant);
        return "lraId:" + lraId + " participants' status:" + participantsString;
    }

    public boolean areAnyInFailedState() {
        for (Participant participant : participants) {
            if (participant.getParticipantStatus() == ParticipantStatus.FailedToComplete ||
                    participant.getParticipantStatus() == FailedToCompensate) {
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
            if (participant.getParticipantStatus() != Compensated && participant.getParticipantStatus() != FailedToCompensate) {
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

    public String getConditionalStringValue(boolean isTrue, String first, String second) {
        return isTrue ? first : second;
    }
}
