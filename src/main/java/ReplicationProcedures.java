
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class ReplicationProcedures {
    // how do we test this?

    @Context
    public GraphDatabaseAPI     db;
    @Context
    public Log                  log;

    @Procedure(value = "replicationStart", mode = Mode.WRITE)
    @Description("starts the replication engine on this server.")
    public void startReplication(URI sourceDbURI) throws InterruptedException {
//      do something.
        ReplicationSwitch.setOn(true);
        // where to put the boolean on and off logic?

        this.scheduledReplicationRunner(sourceDbURI.toString());


    }

    public void stopReplication(){
        // do something.
        ReplicationSwitch.setOn(false);
    }

    private void replicationRunner(String uri){

        while (ReplicationSwitch.isOn()){
            Runnable replicatorThread = new ReplicationThread(db, log, uri);
            // polling mechanism here.
            replicatorThread.run();
        }
    }
    private void scheduledReplicationRunner(String uri) throws InterruptedException {
        while (ReplicationSwitch.isOn())
        {
            ScheduledExecutorService execService
                    =   Executors.newScheduledThreadPool(1);
            execService.scheduleAtFixedRate(()->{
                //The repetitive
                // By using the scheduler in this fashion, I'm not sure we need a runnable task class
                // we might be able to just insert the db interaction code here.
                // I'm thinking we might want to test this in a separate project.
                System.out.println("hi there at: "+ new java.util.Date());
            }, 0, 60L, TimeUnit.SECONDS);


        }
    }


}
