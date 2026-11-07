package org.jgroups.protocols.mongo;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * Tests against a containerized MongoDB database instance.
 *
 * @author Radoslav Husar
 */
public class MONGO_PINGDiscoveryTestCase {

    private static final String CONNECTION_URL_PROPERTY = "jgroups.mongo.connection_url";
    private static final String DATABASE_NAME = "jgroups";
    private static final String DEFAULT_COLLECTION_NAME = "jgroups-ping";
    private static final String CUSTOM_COLLECTION_NAME = "custom-ping-collection";
    private static final long TIMEOUT_MS = 10_000;
    private static final long INTERVAL_MS = 100;

    private static MongoDBContainer mongoDBContainer;
    private static MongoClient mongoClient;

    @BeforeAll
    public static void setUp() {
        assumeTrue(isDockerAvailable(), "Podman/Docker environment is not available - skipping tests against MongoDB container.");

        mongoDBContainer = new MongoDBContainer("mongo:8.0");
        mongoDBContainer.start();

        // Configure the protocol with the container's connection string
        System.setProperty(CONNECTION_URL_PROPERTY, connectionUrl());

        // Client used by the tests to inspect what the protocol stored
        mongoClient = MongoClients.create(mongoDBContainer.getConnectionString());
    }

    @AfterAll
    public static void cleanup() {
        System.clearProperty(CONNECTION_URL_PROPERTY);
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (mongoDBContainer != null) {
            mongoDBContainer.stop();
        }
    }

    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }

    private static String connectionUrl() {
        return mongoDBContainer.getConnectionString() + "/" + DATABASE_NAME;
    }

    public static final int CHANNEL_COUNT = 5;

    // The cluster names need to randomized so that multiple test runs can be run in parallel.
    public static final String RANDOM_CLUSTER_NAME = UUID.randomUUID().toString();

    @FunctionalInterface
    interface ChannelCreator {
        JChannel create() throws Exception;
    }

    @Test
    public void testDiscovery() throws Exception {
        discover(RANDOM_CLUSTER_NAME, this::createFromXml, DEFAULT_COLLECTION_NAME);
    }

    @Test
    public void testDiscoveryObscureClusterName() throws Exception {
        String obscureClusterName = "``\\//--+ěščřžýáíé==''!@#$%^&*()_{}<>?";
        discover(obscureClusterName + RANDOM_CLUSTER_NAME, this::createFromXml, DEFAULT_COLLECTION_NAME);
    }

    @Test
    public void testDiscoveryProgrammaticConfiguration() throws Exception {
        String clusterName = "programmatic-" + RANDOM_CLUSTER_NAME;
        discover(clusterName, this::createProgrammatically, CUSTOM_COLLECTION_NAME);

        // Nothing must have been written to the default collection
        assertEquals(0, countEntries(DEFAULT_COLLECTION_NAME, clusterName), "entries in default collection");
    }

    /**
     * Disconnecting and reconnecting a channel calls stop() and start() on the protocol without init()/destroy(),
     * so the MongoDB client must survive stop().
     */
    @Test
    public void testDisconnectAndReconnect() throws Exception {
        String clusterName = "reconnect-" + RANDOM_CLUSTER_NAME;

        try (JChannel first = createFromXml(); JChannel second = createFromXml()) {
            first.connect(clusterName);
            Util.sleep(1000);
            second.connect(clusterName);
            Util.waitUntilAllChannelsHaveSameView(TIMEOUT_MS, INTERVAL_MS, first, second);

            second.disconnect();
            Util.waitUntil(TIMEOUT_MS, INTERVAL_MS, () -> first.getView().size() == 1, () -> "first channel view: " + first.getView());

            second.connect(clusterName);
            Util.waitUntilAllChannelsHaveSameView(TIMEOUT_MS, INTERVAL_MS, first, second);

            printViews(List.of(first, second));
            assertEquals(2, first.getView().size(), "member count");
            assertEquals(2, second.getView().size(), "member count");
        }
    }

    @Test
    public void testConnectionUrlWithoutDatabaseIsRejected() {
        MONGO_PING ping = new MONGO_PING().setConnectionUrl(mongoDBContainer.getConnectionString());

        IllegalStateException ex = assertThrows(IllegalStateException.class, ping::init);
        assertTrue(ex.getMessage().contains("Database name"), ex.getMessage());
    }

    /**
     * MongoDB failures must be logged rather than propagated out of write() and remove(), as these are called from
     * view handling and stop().
     */
    @Test
    public void testMongoFailuresAreNotPropagated() throws Exception {
        var ping = new FailingMONGO_PING();
        var address = org.jgroups.util.UUID.randomUUID();
        var data = new PingData(address, true, "node", new IpAddress("127.0.0.1", 7800));

        assertDoesNotThrow(() -> ping.callWrite(List.of(data), "failure-" + RANDOM_CLUSTER_NAME));
        assertDoesNotThrow(() -> ping.callRemove("failure-" + RANDOM_CLUSTER_NAME, address));
    }

    /**
     * Fails every MongoDB operation; exposes the inherited JDBC_PING2 write() and remove() which are protected in
     * another package.
     */
    private static class FailingMONGO_PING extends MONGO_PING {
        @Override
        protected MongoCollection<Document> getCollection() {
            throw new MongoException("simulated MongoDB failure");
        }

        void callWrite(List<PingData> list, String clustername) {
            write(list, clustername);
        }

        void callRemove(String clustername, org.jgroups.Address address) {
            remove(clustername, address);
        }
    }

    private void discover(String clusterName, ChannelCreator creator, String collectionName) throws Exception {
        List<JChannel> channels = new LinkedList<>();

        try {
            for (int i = 0; i < CHANNEL_COUNT; i++) {
                JChannel channel = creator.create();
                channels.add(channel);
                channel.connect(clusterName);
                if (i == 0) {
                    // Let's be clear about the coordinator
                    Util.sleep(1000);
                }
            }

            Util.waitUntilAllChannelsHaveSameView(TIMEOUT_MS, INTERVAL_MS, channels.toArray(JChannel[]::new));

            printViews(channels);

            // Asserts the views are there
            for (JChannel channel : channels) {
                assertEquals(CHANNEL_COUNT, channel.getView().getMembers().size(), "member count");
            }

            // Asserts every member is registered in the expected collection
            Util.waitUntil(TIMEOUT_MS, INTERVAL_MS, () -> countEntries(collectionName, clusterName) == CHANNEL_COUNT,
                    () -> "entries in " + collectionName + ": " + countEntries(collectionName, clusterName));
        } finally {
            // n.b. all channels must be closed, only disconnecting all concurrently can leave stale data
            for (JChannel channel : channels) {
                channel.close();
            }
        }

        // Closed members remove their own entries
        Util.waitUntil(TIMEOUT_MS, INTERVAL_MS, () -> countEntries(collectionName, clusterName) == 0,
                () -> "stale entries in " + collectionName + ": " + countEntries(collectionName, clusterName));
    }

    private static long countEntries(String collectionName, String clusterName) {
        return mongoClient.getDatabase(DATABASE_NAME).getCollection(collectionName).countDocuments(eq("clustername", clusterName));
    }

    private JChannel createFromXml() throws Exception {
        return new JChannel("org/jgroups/protocols/mongo/tcp-MONGO_PING.xml");
    }

    private JChannel createProgrammatically() throws Exception {
        var channel = new JChannel(
                new TCP(),
                new MONGO_PING()
                        .setConnectionUrl(connectionUrl())
                        .setCollectionName(CUSTOM_COLLECTION_NAME),
                new MERGE3(),
                new FD_SOCK2(),
                new FD_ALL3(),
                new VERIFY_SUSPECT2(),
                new BARRIER(),
                new NAKACK2(),
                new UNICAST3(),
                new STABLE(),
                new GMS(),
                new MFC(),
                new UFC(),
                new FRAG2(),
                new STATE_TRANSFER()
        );

        // Verify and call the getter
        MONGO_PING mongoPing = channel.getProtocolStack().findProtocol(MONGO_PING.class);
        assertEquals(CUSTOM_COLLECTION_NAME, mongoPing.getCollectionName());

        return channel;
    }

    protected static void printViews(List<JChannel> channels) {
        for (JChannel ch : channels) {
            System.out.println("Channel " + ch.getName() + " has view " + ch.getView());
        }
    }
}
