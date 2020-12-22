package com.neo4j.sync.start;

import com.neo4j.sync.listener.AuditTransactionEventListenerAdapter;
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




@ServiceProvider
public class SyncExtensionFactory extends ExtensionFactory<SyncExtensionFactory.Dependencies>
{
    public SyncExtensionFactory()
    {
        super(ExtensionType.DATABASE, "AuditTransactionEventListenerAdapter");
    }
    @Override
    public Lifecycle newInstance(final ExtensionContext extensionContext, final Dependencies dependencies)
    {
        final GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        final LogService log = dependencies.log();
        final DatabaseManagementService databaseManagementService = dependencies.databaseManagementService();
        return new CustomGraphDatabaseLifecycle(log, db, dependencies, databaseManagementService);
    }
    interface Dependencies
    {
        GraphDatabaseAPI graphdatabaseAPI();
        DatabaseManagementService databaseManagementService();
        AvailabilityGuard availabilityGuard();
        LogService log();
    }
    public static class CustomGraphDatabaseLifecycle extends LifecycleAdapter
    {
        private static Dependencies DEPENDENCIES = null;
        private static DatabaseManagementService DBMS;

        private final GraphDatabaseAPI db1;
        public static GraphDatabaseAPI db2 = null;
        private final Dependencies dependencies;
        private final LogService log;
        private final DatabaseManagementService databaseManagementService;
        AuditTransactionEventListenerAdapter listener;

        public static GraphDatabaseService getDatabase(String databaseName) {
            GraphDatabaseService db = DEPENDENCIES.databaseManagementService().database(databaseName);
            return db;
        }

        public static void dbInit () {
            db2 = (GraphDatabaseAPI) DBMS.database("INTEGRATION.DATABASE");
        }


        public CustomGraphDatabaseLifecycle(final LogService log, final GraphDatabaseAPI db, final Dependencies dependencies,final DatabaseManagementService databaseManagementService)
        {
            this.log = log;
            this.db1 = db;
            this.dependencies = dependencies;
            this.databaseManagementService = databaseManagementService;
            DBMS = databaseManagementService;
            this.DEPENDENCIES = dependencies;



        }

        @Override
        public void start() throws Exception {
//            //log.info("check if auto is enabled and kick off the replication engine");
//            availabilityGuard.addListener(new AvailabilityListener() {
//                @Override
//                public void available() {
//                    if(autoRestart()) {
//                        log.info("Do clever stuff here to check if auto mode is engaged and start the replication engine");
//                    }
//                }
//
//                @Override
//                public void unavailable() {
//                    // Do nothing, the dbms is not available.
//                }
//            });
            System.out.println("calling the start method in the new lifecycle adapter");
//            if (this.db.databaseName().equalsIgnoreCase("neo4j")) {
//                System.out.println("registering the listener with the default database");
//                this.listener = new AuditTransactionEventListenerAdapter();
//                this.databaseManagementService.registerTransactionEventListener(this.db.databaseName(), this.listener);
//            }
        }

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