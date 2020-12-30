package com.neo4j.sync.engine;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.net.ServerAddress;

import java.util.Arrays;
import java.util.HashSet;

public class AddressResolver {

    public static Driver createDriver(String virtualUri, String user, String password, String[] hostNames )
    {
        ServerAddress[] servers = getClusterAddresses(virtualUri, hostNames);
        Config config = Config.builder()
                .withResolver( address -> new HashSet<>( Arrays.asList( servers ) ) )
                .build();

        return GraphDatabase.driver( virtualUri, AuthTokens.basic( user, password ), config );
    }





    private static ServerAddress[] getClusterAddresses(String virtualUri, String[] hostNames) {



        String[] fqdnAndPort = (virtualUri.substring(8,virtualUri.length())).split("\\:");
        String domainName = fqdnAndPort[0].split("\\.", 2)[1];


        ServerAddress[] serverAddresses = new ServerAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            serverAddresses[i] = ServerAddress.of(hostNames[i] + domainName, Integer.getInteger(fqdnAndPort[1]));
        }

        return serverAddresses;
    }
}
