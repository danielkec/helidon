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
package io.helidon.microprofile.lra;

import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.Status;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.WithAnnotations;
import javax.ws.rs.Path;

public class LRACdiExtension implements Extension {
    
    private void registerChannelMethods(
            @Observes
            @WithAnnotations({
                    Path.class,
                    AfterLRA.class,
                    Compensate.class,
                    Complete.class,
                    Forget.class,
                    Status.class
            }) ProcessAnnotatedType<?> pat) {
        // Lookup channel methods
//        LRAParticipant participant = getAsParticipant(pat.getAnnotatedType());
//        if (participant != null) {
//            participants.put(participant.getJavaClass().getName(), participant);
//        }
        
    }
}
