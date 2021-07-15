![NATS](src/main/javadoc/images/large-logo.png)

# NATS - Java Client

A [Java](http://java.com) client for the [NATS messaging system](https://nats.io).

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/nats-io/nats.java.svg?branch=master)](http://travis-ci.org/nats-io/nats.java?branch=master)
[![Coverage Status](https://coveralls.io/repos/nats-io/nats.java/badge.svg?branch=master&service=github)](https://coveralls.io/github/nats-io/nats.java?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.nats/jnats/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.nats/jnats)
[![Javadoc](http://javadoc.io/badge/io.nats/jnats.svg?branch=master)](http://javadoc.io/doc/io.nats/jnats?branch=master)

## A Note on Versions

This is version 2.x of the java-nats library. This version is a ground up rewrite of the original library. Part of the goal of this re-write was to address the excessive use of threads, we created a Dispatcher construct to allow applications to control thread creation more intentionally. This version also removes all non-JDK runtime dependencies.

The API is [simple to use](#listening-for-incoming-messages) and highly [performant](#Benchmarking).

Version 2+ uses a simplified versioning scheme. Any issues will be fixed in the incremental version number. As a major release, the major version has been updated to 2 to allow clients to limit there use of this new API. With the addition of drain() we updated to 2.1, NKey support moved us to 2.2.

The NATS server renamed itself from gnatsd to nats-server around 2.4.4. This and other files try to use the new names, but some underlying code may change over several versions. If you are building yourself, please keep an eye out for issues and report them.

Version 2.5.0 adds some back pressure to publish calls to alleviate issues when there is a slow network. This may alter performance characteristics of publishing apps, although the total performance is equivalent.

Previous versions are still available in the repo.

### SSL/TLS Performance

After recent tests we realized that TLS performance is lower than we would like. After researching the problem and possible solutions we came to a few conclusions:

* TLS performance for the native JDK has not be historically great
* TLS performance is better in JDK12 than JDK8
* A small fix to the library in 2.5.1 allows the use of https://github.com/google/conscrypt and https://github.com/wildfly/wildfly-openssl, conscrypt provides the best performance in our tests
* TLS still comes at a price (1gb/s vs 4gb/s in some tests), but using the JNI libraries can result in a 10x boost in our testing
* If TLS performance is reasonable for your application we recommend using the j2se implementation for simplicity

To use conscrypt or wildfly, you will need to add the appropriate jars to your class path and create an SSL context manually. This context can be passed to the Options used when creating a connection. The NATSAutoBench example provides a conscrypt flag which can be used to try out the library,  manually including the jar is required.

### OCSP Stapling
Our server now supports OCSP stapling. To enable Java to automatically check the stapling 
when making TLS connections, you must set system properties. This can be done from your
command line or from your Java code:

```
System.setProperty("jdk.tls.client.enableStatusRequestExtension", "true");
System.setProperty("com.sun.net.ssl.checkRevocation", "true");
```

For more information, see the Oracle Java documentation page on [Client-Driven OCSP and OCSP Stapling](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ocsp.html)

Also, there is a detailed [OCSP Example](https://github.com/nats-io/java-nats-examples/tree/main/ocsp) that shows how to create SSL contexts enabling OCSP stapling.

### UTF-8 Subjects

The client protocol spec doesn't explicitly state the encoding on subjects. Some clients use ASCII and some use UTF-8 which matches ASCII for a-Z and 0-9. Until 2.1.2 the 2.0+ version of the Java client used ASCII for performance reasons. As of 2.1.2 you can choose to support UTF-8 subjects via the Options. Keep in mind that there is a small performance penalty for UTF-8 encoding and decoding in benchmarks, but depending on your application this cost may be negligible. Also, keep in mind that not all clients support UTF-8 and test accordingly.

### NKey-based Challenge Response Authentication

The NATS server is adding support for a challenge response authentication scheme based on [NKeys](https://github.com/nats-io/nkeys). Version 2.2.0 of
the Java client supports this scheme via an AuthHandler interface. *Version 2.3.0 replaced several NKey methods that used strings with methods using char[]
to improve security.*

## Installation

The java-nats client is provided in a single jar file, with a single external dependency for the encryption in NKey support. See [Building From Source](#building-from-source) for details on building the library.

### Downloading the Jar

You can download the latest jar at [https://search.maven.org/remotecontent?filepath=io/nats/jnats/2.11.6/jnats-2.11.6.jar](https://search.maven.org/remotecontent?filepath=io/nats/jnats/2.11.6/jnats-2.11.6.jar).

The examples are available at [https://search.maven.org/remotecontent?filepath=io/nats/jnats/2.11.6/jnats-2.11.6-examples.jar](https://search.maven.org/remotecontent?filepath=io/nats/jnats/2.11.6/jnats-2.11.6-examples.jar).

To use NKeys, you will need the ed25519 library, which can be downloaded at [https://repo1.maven.org/maven2/net/i2p/crypto/eddsa/0.3.0/eddsa-0.3.0.jar](https://repo1.maven.org/maven2/net/i2p/crypto/eddsa/0.3.0/eddsa-0.3.0.jar).

### Using Gradle

The NATS client is available in the Maven central repository, and can be imported as a standard dependency in your `build.gradle` file:

```groovy
dependencies {
    implementation 'io.nats:jnats:2.11.6'
}
```

If you need the latest and greatest before Maven central updates, you can use:

```groovy
repositories {
    jcenter()
    maven {
        url "https://oss.sonatype.org/content/repositories/releases"
    }
    maven {
        url "https://oss.sonatype.org/content/repositories/snapshots"
    }
}
```

### Using Maven

The NATS client is available on the Maven central repository, and can be imported as a normal dependency in your pom.xml file:

```xml
<dependency>
    <groupId>io.nats</groupId>
    <artifactId>jnats</artifactId>
    <version>2.11.6</version>
</dependency>
```

If you need the absolute latest, before it propagates to maven central, you can use the repository:

```xml
<repositories>
    <repository>
        <id>latest-repo</id>
        <url>https://oss.sonatype.org/content/repositories/releases</url>
        <releases><enabled>true</enabled></releases>
        <snapshots><enabled>false</enabled></snapshots>
    </repository>
</repositories>
```

If you are using the 1.x version of java-nats and don't want to upgrade to 2.0.0 please use ranges in your POM file, java-nats-streaming 1.x is using [1.1, 1.9.9) for this.

## Basic Usage

Sending and receiving with NATS is as simple as connecting to the nats-server and publishing or subscribing for messages. A number of examples are provided in this repo as described in the [Examples Readme](src/examples/java/io/nats/examples/README.md).

### Connecting

There are four different ways to connect using the Java library:

1. Connect to a local server on the default port:

    ```java
    Connection nc = Nats.connect();
    ```

2. Connect to one or more servers using a URL:

    ```java
    //single URL
    Connection nc = Nats.connect("nats://myhost:4222");

    //comma-separated list of URLs
    Connection nc = Nats.connect("nats://myhost:4222,nats://myhost:4223");
    ```

3. Connect to one or more servers with a custom configuration:

    ```java
    Options o = new Options.Builder().server("nats://serverone:4222").server("nats://servertwo:4222").maxReconnects(-1).build();
    Connection nc = Nats.connect(o);
    ```

    See the javadoc for a complete list of configuration options.

4. Connect asynchronously, this requires a callback to tell the application when the client is connected:

    ```java
    Options options = new Options.Builder().server(Options.DEFAULT_URL).connectionListener(handler).build();
    Nats.connectAsynchronously(options, true);
    ```

    This feature is experimental, please let us know if you like it.

5. Connect with authentication handler:

    ```java
    AuthHandler authHandler = Nats.credentials(System.getenv("NATS_CREDS")
    Connection nc = Nats.connect("nats://myhost:4222", authHandler);
    ```

### Publishing

Once connected, publishing is accomplished via one of three methods:

1. With a subject and message body:

    ```java
    nc.publish("subject", "hello world".getBytes(StandardCharsets.UTF_8));
    ```

2. With a subject and message body, as well as a subject for the receiver to reply to:

    ```java
    nc.publish("subject", "replyto", "hello world".getBytes(StandardCharsets.UTF_8));
    ```

3. As a request that expects a reply. This method uses a Future to allow the application code to wait for the response. Under the covers a request/reply pair is the same as a publish/subscribe only the library manages the subscription for you.

    ```java
    Future<Message> incoming = nc.request("subject", "hello world".getBytes(StandardCharsets.UTF_8));
    Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
    String response = new String(msg.getData(), StandardCharsets.UTF_8);
    ```

All of these methods, as well as the incoming message code use byte arrays for maximum flexibility. Applications can
send JSON, Strings, YAML, Protocol Buffers, or any other format through NATS to applications written in a wide range of
languages.

#### ReplyTo When Making A Request

The Message object allows you to set a replyTo, but in requests,
the replyTo is reserved for internal use as the address for the
server to respond to the client with the consumer's reply.

### Listening for Incoming Messages

The Java NATS library provides two mechanisms to listen for messages, three if you include the request/reply discussed above.

1. Synchronous subscriptions where the application code manually asks for messages and blocks until they arrive. Each subscription is associated with a single subject, although that subject can be a wildcard.

    ```java
    Subscription sub = nc.subscribe("subject");
    Message msg = sub.nextMessage(Duration.ofMillis(500));

    String response = new String(msg.getData(), StandardCharsets.UTF_8);
    ```

2. A Dispatcher that will call application code in a background thread. Dispatchers can manage multiple subjects with a single thread and shared callback.

    ```java
    Dispatcher d = nc.createDispatcher((msg) -> {
        String response = new String(msg.getData(), StandardCharsets.UTF_8);
        ...
    });

    d.subscribe("subject");
    ```

    A dispatcher can also accept individual callbacks for any given subscription.

    ```java
    Dispatcher d = nc.createDispatcher((msg) -> {});

    Subscription s = d.subscribe("some.subject", (msg) -> {
        String response = new String(msg.getData(), StandardCharsets.UTF_8);
        System.out.println("Message received (up to 100 times): " + response);
    });
    d.unsubscribe(s, 100);
    ```

## Jetstream

Publishing and subscribing to Jetstream enabled servers is straightforward.  A 
Jetstream enabled application will connect to a server, establish a Jetstream 
context, and then publish or subscribe.  This can be mixed and matched with standard
NATS subject, and JetStream subscribers, depending on configuration, receive messages
from both streams and directly from other NATS producers.

### The Jetstream Context

After establishing a connection as described above, create a Jetstream Context.
   
   ```java
   JetStream js = nc.jetStream();
   ```

You can pass options to configure the Jetstream client, although the defaults should
suffice for most users.  See the `JetStreamOptions` class.

There is no limit to the number of contexts used, although normally one would only
require a single context.  Contexts may be prefixed to be used in conjunction with
NATS authorization.

### Publishing

To publish messages, use the `JetStream.Publish(...)` API.  A stream must be established
before publishing. You can publish in either a synchronous or asynchronous manner.

**Synchronous:**

```java
       // create a typical NATS message
       Message msg = NatsMessage.builder()
               .subject("foo")
               .data("hello", StandardCharsets.UTF_8)
               .build();
      
       PublishAck pa = js.publish(msg);
```

See `NatsJsPub.java` in the JetStream examples for a detailed and runnable example.

If there is a problem an exception will be thrown, and the message may not have been
persisted.  Otherwise, the stream name and sequence number is returned in the publish
acknowledgement.

There are a variety of publish options that can be set when publishing.  When duplicate
checking has been enabled on the stream, a message ID should be set. One set of options
are expectations.  You can set a publish expectation such as a particular stream name,
previous message ID, or previous sequence number.  These are hints to the server that
it should reject messages where these are not met, primarily for enforcing your ordering
or ensuring messages are not stored on the wrong stream.

The PublishOptions are immutable, but the builder an be re-used for expectations by clearing the expected.

For example:

```java
      PublishOptions.Builder pubOptsBuilder = PublishOptions.builder()
              .expectedStream("TEST")
              .messageId("mid1");
      PublishAck pa = js.publish("foo", null, pubOptsBuilder.build());
      
      pubOptsBuilder.clearExpected()
              .setExpectedLastMsgId("mid1")
              .setExpectedLastSequence(1)
              .messageId("mid2");
      pa = js.publish("foo", null, pubOptsBuilder.build());
```

See `NatsJsPubWithOptionsUseCases.java` in the JetStream examples for a detailed and runnable example.

**Asynchronous:**

```java

      List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
      for (int x = 1; x < roundCount; x++) {
          // create a typical NATS message
          Message msg = NatsMessage.builder()
          .subject("foo")
          .data("hello", StandardCharsets.UTF_8)
          .build();

          // Publish a message
          futures.add(js.publishAsync(msg));
     }

     for (CompletableFuture<PublishAck> future : futures) {
         ... process the futures
     }
```

See the `NatsJsPubAsync.java` in the JetStream examples for a detailed and runnable example.

#### ReplyTo When Publishing

The Message object allows you to set a replyTo, but in publish requests,
the replyTo is reserved for internal use as the address for the
server to respond to the client with the PublishAck.

### Subscribing

There are two methods of subscribing, **Push** and **Pull** with each variety having its own set of options and abilities. 

### Push Subscribing

Push subscriptions can be synchronous or asynchronous. The server *pushes* messages to the client.

**Asynchronous:**

```java
        Dispatcher disp = ...;

        MessageHandler handler = (msg) -> {
        // Process the message.
        // Ack the message depending on the ack model
        };

        PushSubscribeOptions so = PushSubscribeOptions.builder()
            .durable("optional-durable-name")
            .build();
        
        boolean autoAck = ...
        
        js.subscribe("my-subject", disp, handler, autoAck);
```

See the `NatsJsPushSubWithHandler.java` in the JetStream examples for a detailed and runnable example.

**Synchronous:**

See `NatsJsPushSub.java` in the JetStream examples for a detailed and runnable example.

### Pull Subscribing

Pull subscriptions are always synchronous. The server organizes messages into a batch
which it sends when requested.

**Fetch:**

```java
        List<Message> message = sub.fetch(100, Duration.ofSeconds(1));
        for (Message m : message) {
            // process message
            m.ack();
        }
```

The fetch pull is a *macro* pull that uses advanced pulls under the covers to return a list of messages.
The list may be empty or contain at most the batch size. All status messages are handled for you.
The client can provide a timeout to wait for the first message in a batch.
The fetch call returns when the batch is ready.
The timeout may be exceeded if the server sent messages very near the end of the timeout period.

See `NatsJsPullSubFetch.java` and `NatsJsPullSubFetchUseCases.java`
in the JetStream examples for a detailed and runnable example.

**Iterate:**

```java
        Iterator<Message> iter = sub.iterate(100, Duration.ofSeconds(1));
        while (iter.hasNext()) {
            Message m = iter.next();
            // process message
            m.ack();
        }
```

The iterate pull is a *macro* pull that uses advanced pulls under the covers to return an iterator.
The iterator may have no messages up to at most the batch size.
All status messages are handled for you.
The client can provide a timeout to wait for the first message in a batch.
The iterate call returns the iterator immediately, but under the covers it will wait for the first
message based on the timeout.
The timeout may be exceeded if the server sent messages very near the end of the timeout period.

See `NatsJsPullSubIterate.java` and `NatsJsPullSubIterateUseCases.java`
in the JetStream examples for a detailed and runnable example.

**Batch Size:**

```java
        sub.pull(100);
        ...
        Message m = sub.nextMessage(Duration.ofSeconds(1));
```

An advanced version of pull specifies a batch size. When asked, the server will send whatever
messages it has up to the batch size. If it has no messages it will wait until it has some to send.
The client may time out before that time. If there are less than the batch size available, 
you can ask for more later. Once the entire batch size has been filled, you must make another pull request. 

See `NatsJsPullSubBatchSize.java` and `NatsJsPullSubBatchSizeUseCases.java` 
in the JetStream examples for detailed and runnable examples.

**No Wait and Batch Size:**

```java
        sub.pullNoWait(100);
        ...
        Message m = sub.nextMessage(Duration.ofSeconds(1));
```

An advanced version of pull also specifies a batch size. When asked, the server will send whatever
messages it has up to the batch size, but will never wait for the batch to fill and the client
will return immediately. If there are less than the batch size available, you will get what is
available and a 404 status message indicating the server did not have enough messages.
You must make a pull request every time. **This is an advanced api** 

See the `NatsJsPullSubNoWaitUseCases.java` in the JetStream examples for a detailed and runnable example.

**Expires In and Batch Size:**

```java
        sub.pullExpiresIn(100, Duration.ofSeconds(3));
        ...
        Message m = sub.nextMessage(Duration.ofSeconds(4));
```

Another advanced version of pull specifies a maximum time to wait for the batch to fill.
The server returns messages when either the batch is filled or the time expires. It's important to
set your client's timeout to be longer than the time you've asked the server to expire in.
You must make a pull request every time. In subsequent pulls, you will receive multiple 408 status
messages, one for each message the previous batch was short. You can just ignore these.
**This is an advanced api**

See `NatsJsPullSubExpire.java` and `NatsJsPullSubExpireUseCases.java`
in the JetStream examples for detailed and runnable examples.

### Message Acknowledgements

There are multiple types of acknowledgements in Jetstream:

* `Message.ack()`: Acknowledges a message.
* `Message.ackSync(Duration)`: Acknowledges a message and waits for a confirmation. When used with deduplications this creates exactly once delivery guarantees (within the deduplication window).  This may significantly impact performance of the system.
* `Message.nak()`: A negative acknowledgment indicating processing failed and the message should be resent later.
* `Message.term()`: Never send this message again, regardless of configuration.
* `Message.inProgress()`:  The message is being processed and reset the redelivery timer in the server.  The message must be acknowledged later when processing is complete.

Note that exactly once delivery guarantee can be achieved by using a consumer with explicit ack mode attached to stream setup with a deduplication window and using the `ackSync` to acknowledge messages.  The guarantee is only valid for the duration of the deduplication window. 

## Advanced Usage

### TLS

NATS supports TLS 1.2. The server can be configured to verify client certificates or not. Depending on this setting the client has several options.

1. The Java library allows the use of the tls:// protocol in its urls. This setting expects a default SSLContext to be set. You can set this default context using System properties, or in code. For example, you could run the publish example using:

    ```bash
    java -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=src/test/resources/truststore.jks -Djavax.net.ssl.trustStorePassword=password io.nats.examples.NatsPub tls://localhost:4443 test "hello world"
    ```

    where the following properties are being set:

    ```bash
    -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks
    -Djavax.net.ssl.keyStorePassword=password
    -Djavax.net.ssl.trustStore=src/test/resources/truststore.jks
    -Djavax.net.ssl.trustStorePassword=password
    ```

    This method can be used with or without client verification.

2. During development, or behind a firewall where the client can trust the server, the library supports the opentls:// protocol which will use a special SSLContext that trusts all server certificates, but provides no client certificates.

    ```bash
    java io.nats.examples.NatsSub opentls://localhost:4443 test 3
    ```

    This method requires that client verification is off.

3. Your code can build an SSLContext to work with or without client verification.

    ```java
    SSLContext ctx = createContext();
    Options options = new Options.Builder().server(ts.getURI()).sslContext(ctx).build();
    Connection nc = Nats.connect(options);
    ```

If you want to try out these techniques, take a look at the [README.md](src/examples/java/io/nats/examples/README.md) for instructions.

Also, here are some places in the code that may help
https://github.com/nats-io/nats.java/blob/main/src/main/java/io/nats/client/support/SSLUtils.java
https://github.com/nats-io/nats.java/blob/main/src/test/java/io/nats/client/TestSSLUtils.java


### Clusters & Reconnecting

The Java client will automatically reconnect if it loses its connection the nats-server. If given a single server, the client will keep trying that one. If given a list of servers, the client will rotate between them. When the nats servers are in a cluster, they will tell the client about the other servers, so that in the simplest case a client could connect to one server, learn about the cluster and reconnect to another server if its initial one goes down.

To tell the connection about multiple servers for the initial connection, use the `servers()` method on the options builder, or call `server()` multiple times.

```Java
String[] serverUrls = {"nats://serverOne:4222", "nats://serverTwo:4222"};
Options o = new Options.Builder().servers(serverUrls).build();
```

Reconnection behavior is controlled via a few options, see the javadoc for the Options.Builder class for specifics on reconnect limits, delays and buffers.

## Benchmarking

The `io.nats.examples` package contains two benchmarking tools, modeled after tools in other NATS clients. Both examples run against an existing nats-server. The first called `io.nats.examples.benchmark.NatsBench` runs two simple tests, the first simply publishes messages, the second also receives messages. Tests are run with 1 thread/connection per publisher or subscriber. Running on an iMac (2017), with 4.2 GHz Intel Core i7 and 64GB of memory produced results like:

```AsciiDoc
Starting benchmark(s) [msgs=5000000, msgsize=256, pubs=2, subs=2]
Current memory usage is 966.14 mb / 981.50 mb / 14.22 gb free/total/max
Use ctrl-C to cancel.
Pub Only stats: 9,584,263 msgs/sec ~ 2.29 gb/sec
 [ 1] 4,831,495 msgs/sec ~ 1.15 gb/sec (2500000 msgs)
 [ 2] 4,792,145 msgs/sec ~ 1.14 gb/sec (2500000 msgs)
  min 4,792,145 | avg 4,811,820 | max 4,831,495 | stddev 19,675.00 msgs
Pub/Sub stats: 3,735,744 msgs/sec ~ 912.05 mb/sec
 Pub stats: 1,245,680 msgs/sec ~ 304.12 mb/sec
  [ 1] 624,385 msgs/sec ~ 152.44 mb/sec (2500000 msgs)
  [ 2] 622,840 msgs/sec ~ 152.06 mb/sec (2500000 msgs)
   min 622,840 | avg 623,612 | max 624,385 | stddev 772.50 msgs
 Sub stats: 2,490,461 msgs/sec ~ 608.02 mb/sec
  [ 1] 1,245,230 msgs/sec ~ 304.01 mb/sec (5000000 msgs)
  [ 2] 1,245,231 msgs/sec ~ 304.01 mb/sec (5000000 msgs)
   min 1,245,230 | avg 1,245,230 | max 1,245,231 | stddev .71 msgs
Final memory usage is 2.02 gb / 2.94 gb / 14.22 gb free/total/max
```

The second, called `io.nats.examples.autobench.NatsAutoBench` runs a series of tests with various message sizes. Running this test on the same iMac, resulted in:

```AsciiDoc
PubOnly 0b           10,000,000          8,464,850 msg/s       0.00 b/s
PubOnly 8b           10,000,000         10,065,263 msg/s     76.79 mb/s
PubOnly 32b          10,000,000         12,534,612 msg/s    382.53 mb/s
PubOnly 256b         10,000,000          7,996,057 msg/s      1.91 gb/s
PubOnly 512b         10,000,000          5,942,165 msg/s      2.83 gb/s
PubOnly 1k            1,000,000          4,043,937 msg/s      3.86 gb/s
PubOnly 4k              500,000          1,114,947 msg/s      4.25 gb/s
PubOnly 8k              100,000            460,630 msg/s      3.51 gb/s
PubSub 0b            10,000,000          3,155,673 msg/s       0.00 b/s
PubSub 8b            10,000,000          3,218,427 msg/s     24.55 mb/s
PubSub 32b           10,000,000          2,681,550 msg/s     81.83 mb/s
PubSub 256b          10,000,000          2,020,481 msg/s    493.28 mb/s
PubSub 512b           5,000,000          2,000,918 msg/s    977.01 mb/s
PubSub 1k             1,000,000          1,170,448 msg/s      1.12 gb/s
PubSub 4k               100,000            382,964 msg/s      1.46 gb/s
PubSub 8k               100,000            196,474 msg/s      1.50 gb/s
PubDispatch 0b       10,000,000          4,645,438 msg/s       0.00 b/s
PubDispatch 8b       10,000,000          4,500,006 msg/s     34.33 mb/s
PubDispatch 32b      10,000,000          4,458,481 msg/s    136.06 mb/s
PubDispatch 256b     10,000,000          2,586,563 msg/s    631.49 mb/s
PubDispatch 512b      5,000,000          2,187,592 msg/s      1.04 gb/s
PubDispatch 1k        1,000,000          1,369,985 msg/s      1.31 gb/s
PubDispatch 4k          100,000            403,314 msg/s      1.54 gb/s
PubDispatch 8k          100,000            203,320 msg/s      1.55 gb/s
ReqReply 0b              20,000              9,548 msg/s       0.00 b/s
ReqReply 8b              20,000              9,491 msg/s     74.15 kb/s
ReqReply 32b             10,000              9,778 msg/s    305.59 kb/s
ReqReply 256b            10,000              8,394 msg/s      2.05 mb/s
ReqReply 512b            10,000              8,259 msg/s      4.03 mb/s
ReqReply 1k              10,000              8,193 msg/s      8.00 mb/s
ReqReply 4k              10,000              7,915 msg/s     30.92 mb/s
ReqReply 8k              10,000              7,454 msg/s     58.24 mb/s
Latency 0b    5,000     35 /  49.20 / 134    +/- 0.77  (microseconds)
Latency 8b    5,000     35 /  49.54 / 361    +/- 0.80  (microseconds)
Latency 32b   5,000     35 /  49.27 / 135    +/- 0.79  (microseconds)
Latency 256b  5,000     41 /  56.41 / 142    +/- 0.90  (microseconds)
Latency 512b  5,000     40 /  56.41 / 174    +/- 0.91  (microseconds)
Latency 1k    5,000     35 /  49.76 / 160    +/- 0.80  (microseconds)
Latency 4k    5,000     36 /  50.64 / 193    +/- 0.83  (microseconds)
Latency 8k    5,000     38 /  55.45 / 206    +/- 0.88  (microseconds)
```

It is worth noting that in both cases memory was not a factor, the processor and OS were more of a consideration. To test this, take a look at the NatsBench results again. Those are run without any constraint on the Java heap and end up doubling the used memory. However, if we run the same test again with a constraint of 1Gb using -Xmx1g, the performance is comparable, differentiated primarily by "noise" that we can see between test runs with the same settings.

```AsciiDoc
Starting benchmark(s) [msgs=5000000, msgsize=256, pubs=2, subs=2]
Current memory usage is 976.38 mb / 981.50 mb / 981.50 mb free/total/max
Use ctrl-C to cancel.

Pub Only stats: 10,123,382 msgs/sec ~ 2.41 gb/sec
 [ 1] 5,068,256 msgs/sec ~ 1.21 gb/sec (2500000 msgs)
 [ 2] 5,061,691 msgs/sec ~ 1.21 gb/sec (2500000 msgs)
  min 5,061,691 | avg 5,064,973 | max 5,068,256 | stddev 3,282.50 msgs

Pub/Sub stats: 3,563,770 msgs/sec ~ 870.06 mb/sec
 Pub stats: 1,188,261 msgs/sec ~ 290.10 mb/sec
  [ 1] 594,701 msgs/sec ~ 145.19 mb/sec (2500000 msgs)
  [ 2] 594,130 msgs/sec ~ 145.05 mb/sec (2500000 msgs)
   min 594,130 | avg 594,415 | max 594,701 | stddev 285.50 msgs
 Sub stats: 2,375,839 msgs/sec ~ 580.04 mb/sec
  [ 1] 1,187,919 msgs/sec ~ 290.02 mb/sec (5000000 msgs)
  [ 2] 1,187,920 msgs/sec ~ 290.02 mb/sec (5000000 msgs)
   min 1,187,919 | avg 1,187,919 | max 1,187,920 | stddev .71 msgs


Final memory usage is 317.62 mb / 960.50 mb / 960.50 mb free/total/max
```

## Building From Source

The build depends on Gradle, and contains `gradlew` to simplify the process. After cloning, you can build the repository and run the tests with a single command:

```bash
> git clone https://github.com/nats-io/nats.java
> cd nats.java
> ./gradlew clean build
```

Or to build without tests

```bash
> ./gradlew clean build -x test
```

This will place the class files in a new `build` folder. To just build the jar:

```bash
> ./gradlew jar
```

The jar will be placed in `build/libs`.

You can also build the java doc, and the samples jar using:

```bash
> ./gradlew javadoc
> ./gradlew exampleJar
```

The java doc is located in `build/docs` and the example jar is in `build/libs`. Finally, to run the tests with the coverage report:

```bash
> ./gradlew test jacocoTestReport
```

which will create a folder called `build/reports/jacoco` containing the file `index.html` you can open and use to browse the coverage. Keep in mind we have focused on library test coverage, not coverage for the examples.

Many of the tests run nats-server on a custom port. If nats-server is in your path they should just work, but in cases where it is not, or an IDE running tests has issues with the path you can specify the nats-server location with the environment variable `nats_-_server_path`.

## TLS Certs

The raw TLS test certs are in [src/test/resources/certs](src/test/resources/certs) and come from the [nats.go](https://github.com/nats-io/nats.go) repository. However, the java client also needs a keystore and truststore.jks files for creating a context. These can be created using:

```bash
> cd src/test/resources
> keytool -keystore truststore.jks -alias CARoot -import -file certs/ca.pem -storepass password -noprompt -storetype pkcs12
> cat certs/client-key.pem certs/client-cert.pem > combined.pem
> openssl pkcs12 -export -in combined.pem -out cert.p12
> keytool -importkeystore -srckeystore cert.p12 -srcstoretype pkcs12 -deststoretype pkcs12 -destkeystore keystore.jks
> keytool -keystore keystore.jks -alias CARoot -import -file certs/ca.pem -storepass password -noprompt
> rm cert.p12 combined.pem
```

## License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.
