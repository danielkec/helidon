/*
 * Copyright (c) 2019 Oracle and/or its affiliates. All rights reserved.
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

/**
 * Eclipse Microprofile Tracing implementation for helidon microprofile.
 */
module io.helidon.jersey.connector {
    requires jersey.client;
    requires jersey.common;
    requires jakarta.activation;
    requires java.logging;
    requires java.ws.rs;
    requires io.helidon.common.reactive;
    requires io.helidon.webclient;
    requires io.netty.codec.http;

    exports io.helidon.jersey.connector;
}
