package io.nats.client.impl;


import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamOptions;
import io.nats.client.Message;
import io.nats.client.api.*;

import java.io.IOException;
import java.util.List;

import static io.nats.client.support.ApiConstants.SEQ;
import static io.nats.client.support.JsonUtils.simpleMessageBody;
import static io.nats.client.support.Validator.*;

public class NatsJetStreamManagement extends NatsJetStreamImplBase implements JetStreamManagement {

    public NatsJetStreamManagement(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        super(connection, jsOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AccountStatistics getAccountStatistics() throws IOException, JetStreamApiException {
        Message resp = makeRequestResponseRequired(JSAPI_ACCOUNT_INFO, null, jso.getRequestTimeout());
        return new AccountStatistics(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo addStream(StreamConfiguration config) throws IOException, JetStreamApiException {
        return addOrUpdateStream(config, JSAPI_STREAM_CREATE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo updateStream(StreamConfiguration config) throws IOException, JetStreamApiException {
        return addOrUpdateStream(config, JSAPI_STREAM_UPDATE);
    }

    private StreamInfo addOrUpdateStream(StreamConfiguration config, String template) throws IOException, JetStreamApiException {
        validateNotNull(config, "Configuration");
        String streamName = config.getName();
        if (nullOrEmpty(streamName)) {
            throw new IllegalArgumentException("Configuration must have a valid stream name");
        }

        String subj = String.format(template, streamName);
        Message resp = makeRequestResponseRequired(subj, config.toJson().getBytes(), jso.getRequestTimeout());
        return new StreamInfo(resp).throwOnHasError();
    }

    @Override
    public boolean deleteStream(String streamName) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        String subj = String.format(JSAPI_STREAM_DELETE, streamName);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo(String streamName) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        String subj = String.format(JSAPI_STREAM_INFO, streamName);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new StreamInfo(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeStream(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_PURGE, streamName);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new PurgeResponse(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo addOrUpdateConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        validateStreamName(streamName, true);
        validateNotNull(config, "Config");
        validateNotNull(config.getDurable(), "Durable"); // durable name is required when creating consumers
        return addOrUpdateConsumerInternal(streamName, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteConsumer(String streamName, String consumer) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        validateNotNull(consumer, "consumer");
        String subj = String.format(JSAPI_CONSUMER_DELETE, streamName, consumer);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }

    @Override
    public ConsumerInfo getConsumerInfo(String streamName, String consumer) throws IOException, JetStreamApiException {
        return super.getConsumerInfo(streamName, consumer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getConsumerNames(String streamName) throws IOException, JetStreamApiException {
        return getConsumerNames(streamName, null);
    }

    // TODO FUTURE resurface this api publicly when server supports
    // @Override
    private List<String> getConsumerNames(String streamName, String filter) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_NAMES, streamName);
        ConsumerNamesReader cnr = new ConsumerNamesReader();
        while (cnr.hasMore()) {
            Message resp = makeRequestResponseRequired(subj, cnr.nextJson(filter), jso.getRequestTimeout());
            cnr.process(resp);
        }
        return cnr.getStrings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ConsumerInfo> getConsumers(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_LIST, streamName);
        ConsumerListReader clg = new ConsumerListReader();
        while (clg.hasMore()) {
            Message resp = makeRequestResponseRequired(subj, clg.nextJson(), jso.getRequestTimeout());
            clg.process(resp);
        }
        return clg.getConsumers();
    }

    @Override
    public List<String> getStreamNames() throws IOException, JetStreamApiException {
        StreamNamesReader snr = new StreamNamesReader();
        while (snr.hasMore()) {
            Message resp = makeRequestResponseRequired(JSAPI_STREAM_NAMES, snr.nextJson(), jso.getRequestTimeout());
            snr.process(resp);
        }
        return snr.getStrings();
    }

    @Override
    public List<StreamInfo> getStreams() throws IOException, JetStreamApiException {
        StreamListReader slg = new StreamListReader();
        while (slg.hasMore()) {
            Message resp = makeRequestResponseRequired(JSAPI_STREAM_LIST, slg.nextJson(), jso.getRequestTimeout());
            slg.process(resp);
        }
        return slg.getStreams();
    }

    @Override
    public MessageInfo getMessage(String streamName, long seq) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        String subj = String.format(JSAPI_MSG_GET, streamName);
        Message resp = makeRequestResponseRequired(subj, simpleMessageBody(SEQ, seq), jso.getRequestTimeout());
        return new MessageInfo(resp).throwOnHasError();
    }

    @Override
    public boolean deleteMessage(String streamName, long seq) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        String subj = String.format(JSAPI_MSG_DELETE, streamName);
        Message resp = makeRequestResponseRequired(subj, simpleMessageBody(SEQ, seq), jso.getRequestTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }
}
