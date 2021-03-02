package com.neo4j.sync.start;

import com.neo4j.sync.engine.Configuration;
import com.neo4j.sync.listener.CaptureTransactionEventListenerAdapter;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.util.List;


/**
 * Protocol is as follows.
 * <p>
 * The SyncExtensionFactory extends the Neo4j database by providing a way to programmatically register
 * and deregister listener adapters, such as our CaptureTransactionEventListenerAdapter.
 * We override methods such as start, shutdown, avialable and unavailable methods to execute
 * logic based on typical database events.
 * </p>
 *
 * @author Chris Upkes
 * @author Jim Webber
 */


@ServiceProvider
public class SyncExtensionFactory extends ExtensionFactory<SyncExtensionFactory.Dependencies> {

    public SyncExtensionFactory()
    {
        super(ExtensionType.DATABASE, "CaptureTransactionEventListenerAdapter");
    }

    @Override
    public Lifecycle newInstance(final ExtensionContext extensionContext, final Dependencies dependencies) {
        final GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        final DatabaseManagementService databaseManagementService = dependencies.databaseManagementService();
        final AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
        return new SynchronizedGraphDatabaseLifecycle(db, dependencies, databaseManagementService);
    }

    interface Dependencies {
        GraphDatabaseAPI graphdatabaseAPI();
        DatabaseManagementService databaseManagementService();
        AvailabilityGuard availabilityGuard();
        LogService log();
    }

    public static class SynchronizedGraphDatabaseLifecycle extends LifecycleAdapter {
        private static Dependencies DEPENDENCIES = null;
        private static DatabaseManagementService DBMS;

        private final GraphDatabaseAPI db1;
        public static GraphDatabaseAPI db2 = null;
        private final Dependencies dependencies;
        private final Log log;
        private final DatabaseManagementService databaseManagementService;
        private final AvailabilityGuard availabilityGuard;

        private List<String> listenerDBs;
        CaptureTransactionEventListenerAdapter listener;

        public SynchronizedGraphDatabaseLifecycle(final GraphDatabaseAPI db, final Dependencies dependencies,
                                                  final DatabaseManagementService databaseManagementService) {
            this.log = db.getDependencyResolver().resolveDependency( LogService.class ).getUserLog( getClass() );
            this.db1 = db;
            this.dependencies = dependencies;
            this.databaseManagementService = databaseManagementService;
            DBMS = databaseManagementService;
            this.DEPENDENCIES = dependencies;
            this.availabilityGuard = dependencies.availabilityGuard();
            initConfig();
        }

        public static GraphDatabaseService getDatabase(String databaseName) {
            return DEPENDENCIES.databaseManagementService().database(databaseName);
        }

        public static void dbInit () {
            db2 = (GraphDatabaseAPI) DBMS.database("INTEGRATION.DATABASE");
        }

        private void initConfig() {
            if (Configuration.isNotInitialized()) {
                Configuration.initializeFromNeoConf(log);
                Configuration.logSettings();
            }
            this.listenerDBs = Configuration.getListenerDBs();
        }

        @Override
        public void start() throws Exception {
            log.info("SynchronizedGraphDatabaseLifecycle -> Starting listener");
            // call a static method to get a handle to the Config object (neo4j.conf)
            availabilityGuard.addListener(new AvailabilityListener() {
                @Override
                public void available() {
                   log.info("SynchronizedGraphDatabaseLifecycle -> DB is available");
                }

                @Override
                public void unavailable() {
                    // Do nothing, the dbms is not available.
                    log.info("SynchronizedGraphDatabaseLifecycle -> DB is no longer available");
                }
            });
            // TODO: determine if we can use methods in the availabilityGuard interface to initialize our app

            for (String database : listenerDBs) {
                if (this.db1.databaseName().equalsIgnoreCase(database)) {
                    log.info("SynchronizedGraphDatabaseLifecycle -> Registering the listener with the '%s' database", database);
                    this.listener = new CaptureTransactionEventListenerAdapter();
                    this.databaseManagementService.registerTransactionEventListener(this.db1.databaseName(), this.listener);
                }
            }

        }

        //        private boolean autoRestart() {
//            // TODO: access the DB for the auto restart config and return boolean
//            return false;
//        }
        @Override
        public void shutdown()
        {
            log.info("SynchronizedGraphDatabaseLifecycle -> Shutting down listener");
            this.databaseManagementService.unregisterTransactionEventListener(this.db1.databaseName(), this.listener);
        }


    }

}