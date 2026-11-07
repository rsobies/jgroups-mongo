package org.jgroups.protocols.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.JDBC_PING2;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

/**
 * Discovery protocol using MongoDB as a shared store for cluster member information.
 * This protocol stores node discovery data (address, name, IP, coordinator status) in a MongoDB collection.
 * <p>
 * Configuration example:
 * <pre>{@code
 * <mongo.MONGO_PING connection_url="mongodb://localhost:27017/jgroups"
 *                   collection_name="jgroups-ping"
 *                   remove_all_data_on_view_change="true"/>
 * }</pre>
 * <p>
 * The connection URL must include the database name (e.g., {@code mongodb://host:port/database}).
 *
 * @author rsobies
 * @author Radoslav Husar
 */
public class MONGO_PING extends JDBC_PING2 {

    // Constants
    protected static final short MONGO_PING_DEFAULT_PROTOCOL_ID = 531;
    private static final String CLUSTERNAME_KEY = "clustername";
    private static final String NAME_KEY = "name";
    private static final String IP_KEY = "ip";
    private static final String ISCOORD_KEY = "isCoord";

    static {
        short protocolId = ClassConfigurator.getProtocolId(MONGO_PING.class);
        // Since JGroups 5.5.3 we can use ClassConfigurator.getProtocolId which manages the ID; until then we need to provide an ID ourselves
        ClassConfigurator.addProtocol(protocolId != 0 ? protocolId : MONGO_PING_DEFAULT_PROTOCOL_ID, MONGO_PING.class);
    }

    @Property(description = "Name of the MongoDB collection used to store cluster member discovery information. Defaults to 'jgroups-ping' collection.", writable = false)
    protected String collection_name = "jgroups-ping";

    // Builder-like methods and method overrides

    @Override
    public MONGO_PING setConnectionUrl(String connectionUrl) {
        this.connection_url = connectionUrl;
        return this;
    }

    public String getCollectionName() {
        return collection_name;
    }

    public MONGO_PING setCollectionName(String collectionName) {
        this.collection_name = collectionName;
        return this;
    }

    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    @Override
    public void init() throws Exception {
        var connectionString = new ConnectionString(connection_url);
        if (connectionString.getDatabase() == null) {
            throw new IllegalStateException("Database name must be specified in connection_url");
        }
        mongoClient = MongoClients.create(connectionString);
        try {
            collection = mongoClient.getDatabase(connectionString.getDatabase()).getCollection(collection_name);
            super.init();
        } catch (Exception e) {
            closeClient();
            throw e;
        }
    }

    protected MongoCollection<Document> getCollection() {
        return this.collection;
    }

    /**
     * Closes the client in destroy() rather than stop(): a channel can be disconnected (stop) and reconnected (start)
     * multiple times, while init() which creates the client is only called once per stack.
     * JDBC_PING2.stop() also still uses the client to remove the local entry.
     */
    @Override
    public void destroy() {
        super.destroy();
        closeClient();
    }

    private void closeClient() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
            collection = null;
        }
    }

    /**
     * MongoDB driver exceptions are unchecked, while JDBC_PING2 (e.g., write() and remove()) only handles
     * {@link SQLException}; wrap them so failures are logged by the parent rather than propagated.
     */
    private static SQLException toSQLException(MongoException e) {
        return new SQLException(e.getMessage(), e);
    }

    // No-op overrides otherwise inherited and called from JDBC_PING2.init()

    @Override
    protected void loadDriver() {
        // No-op.
    }

    @Override
    protected void createInsertStoredProcedure() {
        // No-op.
    }

    @Override
    protected void clearTable(String clustername) {
        // Not called by JDBC_PING2 (its removeAll() is unsupported); overridden so it doesn't hit the null JDBC connection
        getCollection().deleteMany(eq(CLUSTERNAME_KEY, clustername));
    }

    /**
     * Returns null as MongoDB does not use JDBC connections. The parent class methods that receive
     * a Connection parameter (e.g., {@link #delete(Connection, String, Address)}) are overridden
     * to ignore it and use the cached {@link MongoCollection} instead.
     */
    @Override
    protected Connection getConnection() {
        return null;
    }

    @Override
    protected void writeToDB(PingData data, String clustername) throws SQLException {
        Address address = data.getAddress();
        String addr = Util.addressToString(address);
        String name = address instanceof SiteUUID ? ((SiteUUID) address).getName() : NameCache.get(address);
        PhysicalAddress ip_addr = data.getPhysicalAddr();
        String ip = ip_addr.toString();

        var filter = and(eq("_id", addr), eq(CLUSTERNAME_KEY, clustername));
        var document = new Document("_id", addr)
                .append(NAME_KEY, name)
                .append(CLUSTERNAME_KEY, clustername)
                .append(IP_KEY, ip)
                .append(ISCOORD_KEY, data.isCoord());
        try {
            getCollection().replaceOne(filter, document, new ReplaceOptions().upsert(true));
        } catch (MongoException e) {
            throw toSQLException(e);
        }
    }

    @Override
    protected void createSchema() {
        try {
            var db = mongoClient.getDatabase(collection.getNamespace().getDatabaseName());
            db.createCollection(collection_name);
        } catch (MongoCommandException mex) {
            // Ignore "collection already exists" error (code 48)
            if (mex.getErrorCode() != 48) {
                throw mex;
            }
        }
    }

    @Override
    protected List<PingData> readFromDB(String cluster) throws Exception {
        try (var iterator = getCollection().find(eq(CLUSTERNAME_KEY, cluster)).iterator()) {
            // Lock only for the shared counter to work around its non-volatility
            lock.lock();
            try {
                reads++;
            } finally {
                lock.unlock();
            }
            List<PingData> retval = new LinkedList<>();

            while (iterator.hasNext()) {
                var doc = iterator.next();
                String uuid = doc.get("_id", String.class);
                Address address = Util.addressFromString(uuid);
                String name = doc.get(NAME_KEY, String.class);
                String ip = doc.get(IP_KEY, String.class);
                IpAddress ipAddress = new IpAddress(ip);
                boolean isCoord = Boolean.TRUE.equals(doc.get(ISCOORD_KEY, Boolean.class));
                PingData pingData = new PingData(address, true, name, ipAddress).coord(isCoord);
                retval.add(pingData);
            }

            return retval;
        }
    }

    @Override
    protected void delete(Connection conn, String clustername, Address addressToDelete) throws SQLException {
        // Ignore conn - it's null anyway
        this.delete(clustername, addressToDelete);
    }

    @Override
    protected void delete(String clustername, Address addressToDelete) throws SQLException {
        String address = Util.addressToString(addressToDelete);
        try {
            getCollection().deleteOne(and(eq("_id", address), eq(CLUSTERNAME_KEY, clustername)));
        } catch (MongoException e) {
            throw toSQLException(e);
        }
    }
}
