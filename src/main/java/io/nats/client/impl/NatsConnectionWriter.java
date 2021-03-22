// Copyright 2015-2018 The NATS Authors
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

package io.nats.client.impl;

import io.nats.client.Options;
import io.nats.client.impl.MessageQueue.AccumulateResult;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.support.NatsConstants.*;

class NatsConnectionWriter implements Runnable {

    private final NatsConnection connection;

    private Future<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
    private DataPort dataPort = null;
    private final AtomicBoolean running;
    private final AtomicBoolean reconnectMode;
    private final ReentrantLock startStopLock;

    private final int maxWriteSize;
    private final ByteArrayBuilder sendBuilder;

    private WriteMessageQueue outgoing;
    private WriteMessageQueue reconnectOutgoing;

    NatsConnectionWriter(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.reconnectMode = new AtomicBoolean(false);
        this.startStopLock = new ReentrantLock();
        this.stopped = new CompletableFuture<>();
        ((CompletableFuture<Boolean>)this.stopped).complete(Boolean.TRUE); // we are stopped on creation

        Options options = connection.getOptions();
        maxWriteSize = options.getBufferSize();
        sendBuilder = new ByteArrayBuilder(maxWriteSize);

        outgoing = new WriteMessageQueue(true,
            options.getMaxMessagesInOutgoingQueue(),
            options.isDiscardMessagesWhenOutgoingQueueFull());

        // The reconnect buffer contains internal messages, and we will keep it unlimited in size
        reconnectOutgoing = new WriteMessageQueue(true, 0);
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.startStopLock.lock();
        try {
            this.dataPortFuture = dataPortFuture;
            this.running.set(true);
            this.outgoing.resume();
            this.reconnectOutgoing.resume();
            this.stopped = connection.getExecutor().submit(this, Boolean.TRUE);
        } finally {
            this.startStopLock.unlock();
        }
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop() {
        this.running.set(false);
        this.startStopLock.lock();
        try {
                this.outgoing.pause();
                this.reconnectOutgoing.pause();
                // Clear old ping/pong requests
                this.outgoing.filter(msg ->
                        msg.getProtocolBytes().equals(OP_PING_BYTES) || msg.getProtocolBytes().equals(OP_PONG_BYTES));

        } finally {
                this.startStopLock.unlock();
        }
        
        return this.stopped;
    }

    synchronized void sendMessageBatch(AccumulateResult result, DataPort dataPort, NatsStatistics stats) throws IOException {
        sendBuilder.clear();

        NatsMessage msg = result.head;
        sendBuilder.ensureCapacity(result.size);

        while (msg != null) {
            msg.appendProtocolTo(sendBuilder);
            sendBuilder.append(CRLF_BYTES);

            if (msg.isRegular()) {
                if (msg.hasHeaders()) {
                    msg.getHeaders().appendTo(sendBuilder);
                }

                byte[] bytes = msg.getData(); // guaranteed to not be null
                if (bytes != null && bytes.length > 0) {
                    sendBuilder.append(bytes);
                }
                sendBuilder.append(CRLF_BYTES);
            }

            msg = msg.next;
            if (msg != null) {
                int len = sendBuilder.length();
                if (len >= maxWriteSize) {
                    write(dataPort, stats, len);
                    sendBuilder.clear();
                }
            }
        }
        write(dataPort, stats, sendBuilder.length());
        stats.addOutMsgs(result.count);
    }

    private void write(DataPort dataPort, NatsStatistics stats, int len) throws IOException {
        if (len > 0) {
            dataPort.write(sendBuilder.internalArray(), len);
            connection.getNatsStatistics().registerWrite(len);
            stats.addOutBytes(len);
        }
    }

    @Override
    public void run() {
        Duration waitForMessage = Duration.ofMinutes(2); // This can be long since no one is sending
        Duration reconnectWait = Duration.ofMillis(1); // This should be short, since we are trying to get the reconnect through

        try {
            dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            NatsStatistics stats = this.connection.getNatsStatistics();
            int maxAccumulate = Options.MAX_MESSAGES_IN_NETWORK_BUFFER;

            while (this.running.get()) {
                AccumulateResult result;
                
                if (this.reconnectMode.get()) {
                    result = this.reconnectOutgoing.accumulate(maxWriteSize, maxAccumulate, reconnectWait);
                } else {
                    result = this.outgoing.accumulate(maxWriteSize, maxAccumulate, waitForMessage);
                }

                if (result == null) { // Make sure we are still running
                    continue;
                }

                sendMessageBatch(result, dataPort, stats);
            }
        } catch (IOException | BufferOverflowException io) {
            this.connection.handleCommunicationIssue(io);
        } catch (CancellationException | ExecutionException | InterruptedException ex) {
            // Exit
        } finally {
            this.running.set(false);
        }
    }

    void setReconnectMode(boolean tf) {
        reconnectMode.set(tf);
    }

    boolean canQueue(NatsMessage msg, long maxSize) {
        return (maxSize < 0 || (outgoing.sizeInBytes() + msg.getSizeInBytes()) < maxSize);
    }

    boolean queue(NatsMessage msg) {
        return this.outgoing.push(msg, false);
    }

    void queueInternalMessage(NatsMessage msg) {
        if (this.reconnectMode.get()) {
            this.reconnectOutgoing.push(msg, false);
        } else {
            this.outgoing.push(msg, true);
        }
    }

    synchronized void flushBuffer() {
        // Since there is no connection level locking, we rely on syncronization
        // of the APIs here.
        try  {
            if (this.running.get()) {
               dataPort.flush();
            }
        } catch (Exception e) {
            // NOOP;
        }
    }
}
