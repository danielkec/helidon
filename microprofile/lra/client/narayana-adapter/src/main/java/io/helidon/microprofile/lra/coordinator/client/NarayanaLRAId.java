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

package io.helidon.microprofile.lra.coordinator.client;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.UriBuilder;

class NarayanaLRAId {

    static Pattern LRA_ID_PATTERN = Pattern.compile(".*/([^/?]+).*");

    /**
     * Narayana sends coordinator url as part of lraId and sometimes even parentLRA as query param:
     * http://127.0.0.1:8070/lra-coordinator/0_ffff7f000001_a76d_608fb07d_183a?ParentLRA=http%3A%2F%2...
     * <p>
     * Helidon client impl works with clean lraId, no unnecessary magic is needed.
     *
     * @param narayanaLRAId narayana lraId with uid hidden inside
     * @return uid of LRA
     */
    static URI parseLRAId(String narayanaLRAId) {
        Matcher m = LRA_ID_PATTERN.matcher(narayanaLRAId);
        if (!m.matches()) {
            //LRA id format from Narayana
            throw new RuntimeException("Error when parsing Narayana lraId: " + narayanaLRAId);
        }
        return UriBuilder.fromPath(m.group(1)).build();
    }
}
