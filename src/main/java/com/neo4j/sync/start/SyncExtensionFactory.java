package com.neo4j.sync.start;

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
import org.neo4j.logging.internal.LogService;

/**
 * <p>
 * The extension factory is used to automate the registration of our transaction event listener.
 * It can also be used to execute logic on startup, available, not available and shutdown events.
 * </p>
 *
 * @author Chris Upkes
 * @author Jim Webber
 */

@ServiceProvider
public class SyncExtensionFactory extends ExtensionFactory<SyncExtensionFactory.Dependencies>
{
    public SyncExtensionFactory()
    {
        super(ExtensionType.DATABASE, "CaptureTransactionEventListenerAdapter");
    }
    @Override
    public Lifecycle newInstance(final ExtensionContext extensionContext, final Dependencies dependencies)
    {
        final GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        final LogService log = dependencies.log();
        final DatabaseManagementService databaseManagementService = dependencies.databaseManagementService();
        final AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
        return new SynchronizedGraphDatabaseLifecycle(log, db, dependencies, databaseManagementService);
    }
    interface Dependencies
    {
        GraphDatabaseAPI graphdatabaseAPI();
        DatabaseManagementService databaseManagementService();
        AvailabilityGuard availabilityGuard();
        LogService log();
    }
    public static class SynchronizedGraphDatabaseLifecycle extends LifecycleAdapter
    {
        // we made some of the variables static because we were attempting
        // to reference them inside of the available method.
        private static Dependencies DEPENDENCIES = null;
        private static DatabaseManagementService DBMS;

        private final GraphDatabaseAPI db1;
        private final LogService log;
        private final DatabaseManagementService databaseManagementService;
        private final AvailabilityGuard availabilityGuard;
        private static boolean settingsInitialized = false;


        CaptureTransactionEventListenerAdapter listener;

        public static GraphDatabaseService getDatabase(String databaseName) {
            GraphDatabaseService db = DEPENDENCIES.databaseManagementService().database(databaseName);
            return db;
        }

        public static GraphDatabaseService getDatabase() {

            return (GraphDatabaseService) DBMS;
        }

        public SynchronizedGraphDatabaseLifecycle(final LogService log, final GraphDatabaseAPI db, final Dependencies dependencies,final DatabaseManagementService databaseManagementService)
        {
            this.log = log;
            this.db1 = db;
            this.databaseManagementService = databaseManagementService;
            DBMS = databaseManagementService;
            this.DEPENDENCIES = dependencies;
            this.availabilityGuard = dependencies.availabilityGuard();
        }

        @Override
        public void start() throws Exception {
            //log.info("check if auto is enabled and kick off the replication engine");
            availabilityGuard.addListener(new AvailabilityListener() {
                @Override
                public void available() {

                    System.out.println("Some db is available");

                    if (!settingsInitialized) {

                        // TODO: write some init code here
                        // cannot get a handle to a database and read or write

                    }
                }

                @Override
                public void unavailable() {
                    // Do nothing, the dbms is not available.
                    System.out.println("Some db is no longer available");
                }
            });

            System.out.println("calling the start method in the new lifecycle adapter");
            // TODO: replace the event registration in the tests with event registration here
            // change the name of the database to be the Federos database

//            if (this.db.databaseName().equalsIgnoreCase("neo4j")) {
//                System.out.println("registering the listener with the default database");
//                this.listener = new CaptureTransactionEventListenerAdapter();
//                this.databaseManagementService.registerTransactionEventListener(this.db.databaseName(), this.listener);
//            }
        }
          // I'm not sure about this new method.  Jim authored this and I need to speak with him regarding it's use
//        private boolean autoRestart() {
//            // TODO: access the DB for the auto restart config and return boolean
//            return false;
//        }

        @Override
        public void shutdown()
        {
            System.out.println("calling the shutdown method in the new lifecycle adapter");
            //this.databaseManagementService.unregisterTransactionEventListener(this.db.databaseName(), this.listener);
        }


    }

}