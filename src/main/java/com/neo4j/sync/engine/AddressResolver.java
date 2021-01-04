package com.neo4j.sync.engine;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.net.ServerAddress;
import java.util.Arrays;
import java.util.HashSet;

/**
 * com.neo4j.sync.engine.Address Resolver allows you to provide a virtual uri, username, password and array of hostnames
 * to construct a set of real URIs that the resolver can use to connect to an available host in a cluster.
 * The class encapsulates the logic provided by Neo4j to build and pass configuration information
 * to the driver method of the GraphDatabase object.
 *
 * @author Chris Upkes
 */

public class AddressResolver {

    public static Driver createDriver(String virtualUri, String user, String password, String[] hostNames )
    {
        // *** UNTESTED ***
        // pass the array of host names and get back ServerAddress objects
        ServerAddress[] servers = getClusterAddresses(virtualUri, hostNames);
        // build the configuration object with a resolver
        Config config = Config.builder()
                .withResolver( address -> new HashSet<>( Arrays.asList( servers ) ) )
                .build();
        // the driver construction method will now use the configuration to
        // round-robin choose an available host to establish a connection.
        return GraphDatabase.driver( virtualUri, AuthTokens.basic( user, password ), config );
    }

    private static ServerAddress[] getClusterAddresses(String virtualUri, String[] hostNames) {

        // *** UNTESTED ***
        // quick logic to grab the fully qualified domain name and port (sans the protocol)
        String[] fqdnAndPort = (virtualUri.substring(8)).split(":");
        // remove the bogus hostname to get the domain name.
        String domainName = fqdnAndPort[0].split("\\.", 2)[1];
        // the configuration requires ServerAddress
        ServerAddress[] serverAddresses = new ServerAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            serverAddresses[i] = ServerAddress.of(hostNames[i] + domainName, Integer.getInteger(fqdnAndPort[1]));
        }
        return serverAddresses;
    }
}
