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

import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * Subscription on a JetStream context.
 */
public interface JetStreamSubscription extends Subscription {

    /**
     * Polls for new messages, overriding the default batch size for this pull only.
     * This should only be used when the subscription is pull based.
     *
     * @param batchSize the size of the batch
     */
    void pull(int batchSize);

    /**
     * Do a pull in noWait mode with the specified batch size.
     *
     * When true a response with a 404 status header will be returned
     * when no messages are available.
     * This can only be used when the subscription is pull based.
     *
     * @param batchSize the size of the batch
     */
    void pullNoWait(int batchSize);

    /**
     * Initiate pull for all messages available before expiration.
     * This can only be used when the subscription is pull based.
     * <p>
     * <code>sub.nextMessage(timeout)</code> can return a:
     * <ul>
     * <li>regular message
     * <li>null
     * <li>408 status message
     * </ul>
     * <p>
     * Multiple 408 status messages may come. Each one indicates a
     * missing item from the previous batch and can be discarded.
     * <p>
     *
     * @param batchSize the size of the batch
     * @param expiresIn how long from now this request should be expired from the server wait list
     */
    void pullExpiresIn(int batchSize, Duration expiresIn);

    /**
     * Fetch a list of messages up to the batch size, waiting no longer than maxWait.
     * This uses <code>pullExpiresIn</code> under the covers, and manages all responses
     * from <code>sub.nextMessage(...)</code> to only return regular JetStream messages.
     * This can only be used when the subscription is pull based.
     *
     * @param batchSize the size of the batch
     * @param maxWait the maximum time to wait for the first message.
     *
     * @return the list of messages
     */
    List<Message> fetch(int batchSize, Duration maxWait);

    /**
     * Prepares an iterator. This uses <code>pullExpiresIn</code> under the covers,
     * and manages all responses. The iterator will have no messages if it does not
     * receive the first message within the max wait period. It will stop if the batch is
     * fulfilled or if there are fewer than batch size messages. 408 Status messages
     * are ignored and will not count toward the fulfilled batch size.
     *
     * @param batchSize the size of the batch
     * @param maxWait the maximum time to wait for the first message.
     *
     * @return the message iterator
     */
    Iterator<Message> iterate(final int batchSize, Duration maxWait);

    /**
     * Gets information about the consumer behind this subscription.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;
}
