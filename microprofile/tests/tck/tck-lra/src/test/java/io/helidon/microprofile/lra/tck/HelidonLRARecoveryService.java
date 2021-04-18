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

package io.helidon.microprofile.lra.tck;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import io.helidon.microprofile.lra.coordinator.Coordinator;

import org.eclipse.microprofile.lra.tck.service.spi.LRARecoveryService;

public class HelidonLRARecoveryService implements LRARecoveryService {

    @Override
    public void waitForCallbacks(URI lraId) {
    }

    @Override
    public boolean waitForEndPhaseReplay(URI lraId) {
        Coordinator.nextRecoveryCycle().await(10, TimeUnit.SECONDS);
        return true;
    }
}