package com.neo4j.sync.engine;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.net.ServerAddress;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * com.neo4j.sync.engine.Address Resolver allows you to provide a virtual uri, username, password and array of hostnames
 * to construct a set of real URIs that the resolver can use to connect to an available host in a cluster.
 * The class encapsulates the logic provided by Neo4j to build and pass configuration information
 * to the driver method of the GraphDatabase object.
 *
 * @author Chris Upkes
 */

public class AddressResolver {

    public static Driver createDriver(String virtualUri, String user, String password, Set<String> hostNames ) throws URISyntaxException
    {
        // *** UNTESTED ***
        // pass the array of host names and get back ServerAddress objects
        Set<ServerAddress> servers = getClusterAddresses(virtualUri, hostNames);
        // build the configuration object with a resolver
        Config config = Config.builder()
                .withResolver( address -> servers )
                .build();
        // the driver construction method will now use the configuration to
        // round-robin choose an available host to establish a connection.
        return GraphDatabase.driver( virtualUri, AuthTokens.basic( user, password ), config );
    }

    private static Set<ServerAddress> getClusterAddresses(String virtualUri, Set<String> hostNames) throws URISyntaxException {
        // *** UNTESTED ***
        URI uri = new URI(virtualUri);
        Set<ServerAddress> serverAddresses = new HashSet<>();

        for (String hostName : hostNames) {
            serverAddresses.add(ServerAddress.of(hostName, uri.getPort()));
        }
        return serverAddresses;
    }
}
