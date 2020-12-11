package com.neo4j.sync.engine;


import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import com.neo4j.sync.engine.ReplicationEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

public class GraphWriter {

    private JSONObject graphTransaction;
    private String changeType;
    private  GraphDatabaseAPI graphDb;
    private Log log;

    public GraphWriter(JSONObject graphTransaction){

        this.graphTransaction = graphTransaction;

    }

    private void delegateCRUDOperation() throws JSONException {

        this.changeType = this.graphTransaction.get("changeType").toString();

        switch (changeType) {
            case "addNode": this.addNodes();
            case "deleteNode": this.deleteNodes();
            case "addRelation": this.addRelation();
            case "deleteRelation": this.deleteRelation();
        }

    }

    public void writeGraphTransaction() throws JSONException {

        this.delegateCRUDOperation();

    }
    private void addNodes(){

        try (Transaction tx = graphDb.beginTx()) {
            Node newNode = tx.createNode(Label.label("Test"));
            tx.commit();
        } catch (Exception e) {
            // log exception
            //this.logException(e, databaseService);
            System.out.println(e.getMessage());

        } finally
        {
            log.info("proc write complete");


        }




    }

    private void deleteNodes(){

    }

    private void addRelation(){


    }

    private void deleteRelation(){

    }


}
