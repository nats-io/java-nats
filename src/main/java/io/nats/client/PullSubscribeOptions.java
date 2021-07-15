// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client;

import io.nats.client.api.ConsumerConfiguration;

import static io.nats.client.support.Validator.*;

/**
 * The PullSubscribeOptions class specifies the options for subscribing with JetStream enabled servers.
 * Options are created using the constructors or a {@link Builder}.
 */
public class PullSubscribeOptions extends SubscribeOptions {

    private PullSubscribeOptions(String stream, boolean direct, ConsumerConfiguration consumerConfig) {
        super(stream, direct, consumerConfig);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create PullSubscribeOptions where you are binding to
     * a specific stream, specific durable and are using direct mode
     * @param stream the stream name to bind to
     * @param durable the durable name
     * @return push subscribe options
     */
    public static PullSubscribeOptions direct(String stream, String durable) {
        return new PullSubscribeOptions.Builder().stream(stream).durable(durable).direct().build();
    }

    /**
     * PullSubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder
            extends SubscribeOptions.Builder<Builder, PullSubscribeOptions> {

        @Override
        protected Builder getThis() {
            return this;
        }

        /**
         * Builds the pull subscribe options.
         * @return pull subscribe options
         */
        @Override
        public PullSubscribeOptions build() {
            // stream not required when not direct (direct is checked later)
            stream = validateStreamName(stream, false);

            // durable is required for pull
            durable = validateDurableRequired(durable, consumerConfig);

            validateDirect(direct, stream, durable);

            ConsumerConfiguration cc = ConsumerConfiguration.builder(consumerConfig)
                    .durable(durable)
                    .build();

            return new PullSubscribeOptions(stream, direct, cc);
        }
    }
}
