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

package io.helidon.microprofile.scheduling;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

class FixedRateTask implements Task {

    private static final Logger LOGGER = Logger.getLogger(FixedRateTask.class.getName());
    private final long initialDelay;
    private final long delay;
    private final TimeUnit timeUnit;
    private final Task.InternalTask actualTask;

    FixedRateTask(ScheduledExecutorService executorService,
                  long initialDelay,
                  long delay,
                  TimeUnit timeUnit,
                  Task.InternalTask actualTask) {
        this.initialDelay = initialDelay;
        this.delay = delay;
        this.timeUnit = timeUnit;
        this.actualTask = actualTask;
        executorService.scheduleAtFixedRate(this, initialDelay, delay, timeUnit);
    }

    @Override
    public String description() {
        if (initialDelay == 0) {
            return String.format("Every %s %s", delay, timeUnit.toString());
        }
        return String.format("Every %s %s with initial delay %s %s",
                delay, timeUnit.toString(), initialDelay, timeUnit.toString());
    }

    @Override
    public void run() {
        try {
            actualTask.run();
        } catch (Throwable e) {
            LOGGER.log(Level.SEVERE, e, () -> "Error when invoking scheduled method.");
        }
    }
}
