import org.neo4j.driver.*;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.List;

public class ReplicationThread  implements Runnable {
    private final GraphDatabaseAPI db;
    private final Log log;
    private final String sourceDBURI;
    private Driver driver;
    private String database;

    public ReplicationThread(GraphDatabaseAPI db, Log log, String sourceDBURI) {
        this.db = db;
        this.log = log;
        this.sourceDBURI = sourceDBURI;
        this.driver = GraphDatabase.driver(this.sourceDBURI,
               AuthTokens.basic("foo", "bar")
        );
    }


    @Override
    public void run() {

        //connect to remote

        try (Session replicationSession = getSession()) {

//
            Result txResult = replicationSession.run("MATCH (tr:TransactionRecord) RETURN tr.transactionId, transactionData");

//            org.neo4j.graphdb.Transaction tx = db.beginTx();
//            // break down json into nodes and relationships
//            //
//
//            try {
//
//                tx.commit();
//
//            } catch (Exception e) {
//                tx.rollback();
//            }
            System.out.println(txResult.stream().findFirst().toString());


        } catch (Exception e) {
            // log exception
        }





    }

    private Session getSession (){
        return driver.session();
    }

    private List<Node> nodeListBuilder (String transactionData) {
        // extract nodes to be created
        return null;
    }

}
