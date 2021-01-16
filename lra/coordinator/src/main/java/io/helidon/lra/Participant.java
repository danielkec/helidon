package io.helidon.lra;

import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

import java.net.URI;

import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.*;
import static org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;

public class Participant {

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

    void setParticipantStatus(ParticipantStatus participantStatus) {
        this.participantStatus = participantStatus;
    }

    URI getCompleteURI() {
        return completeURI;
    }

    void setCompleteURI(URI completeURI) {
        this.completeURI = completeURI;
    }

    URI getCompensateURI() {
        return compensateURI;
    }

    void setCompensateURI(URI compensateURI) {
        this.compensateURI = compensateURI;
    }

    URI getAfterURI() {
        return afterURI;
    }

    void setAfterURI(URI afterURI) {
        this.afterURI = afterURI;
    }

    URI getForgetURI() {
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

    void setForgotten() {
        this.isForgotten = true;
    }

    boolean isForgotten() {
        return isForgotten;
    }

    void setAfterLRASuccessfullyCalledIfEnlisted() {
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
}

/**
 [ERROR] Failures:
 [ERROR]   TckTests.completeMultiLevelNestedActivity:167->multiLevelNestedActivity:659 multiLevelNestedActivity: step 5 (called test path http://localhost:8180/lraresource/multiLevelNestedActivity) expected:<2> but was:<3>
 [ERROR]   TckTests.mixedMultiLevelNestedActivity:177->multiLevelNestedActivity:685 multiLevelNestedActivity: step 10 (called test path http://localhost:8180/lraresource/multiLevelNestedActivity) expected:<3> but was:<5>


 [ERROR] Failures:
 [ERROR]   TckContextTests.testForgetCalledForNestedParticipantsWhenParentIsClosed:237 resource should have called forget for the nested LRA expected:<1> but was:<0>
 [ERROR]   TckContextTests.testParentContextAvailable:205 when the resource was asked to complete a nested LRA the parent context header was missing expected:<1> but was:<0>
 [ERROR]   TckParticipantTests.testNonJaxRsCompletionStageResponseAndParticipantStatus:175 Non JAX-RS @Status method with CompletionStage<ParticipantStatus> should have been called
 Expected: a value equal to or greater than <1>
 but: <0> was less than <1>
 [ERROR]   TckTests.compensateMultiLevelNestedActivity:172->multiLevelNestedActivity:677 multiLevelNestedActivity: step 8 (called test path http://localhost:8180/lraresource/multiLevelNestedActivity) expected:<2> but was:<1>
 [INFO]
 [ERROR] Tests run: 133, Failures: 4, Errors: 0, Skipped: 0


 [ERROR]   TckTests.compensateMultiLevelNestedActivity:172->multiLevelNestedActivity:677 multiLevelNestedActivity: step 8 (called test path http://localhost:8180/lraresource/multiLevelNestedActivity) expected:<2> but was:<1>
 [ERROR]   TckTests.mixedMultiLevelNestedActivity:177->multiLevelNestedActivity:693 multiLevelNestedActivity: step 9 (called test path http://localhost:8180/lraresource/multiLevelNestedActivity) expected:<1> but was:<0>
 [INFO]
 [ERROR] Tests run: 23, Failures: 2, Errors: 0, Skipped: 0
 */

