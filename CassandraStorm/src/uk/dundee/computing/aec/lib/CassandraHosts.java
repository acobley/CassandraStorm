package uk.dundee.computing.aec.lib;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

/**********************************************************
 * 
 * 
 * @author administrator
 *
 *         Hosts are 192.168.2.10 Seed for Vagrant hosts
 * 
 * 
 *
 *
 */

public final class CassandraHosts  {
	private static Cluster cluster;
	//static String Host = "192.168.2.10"; // at least one starting point to talk
											// to
static String Host = "127.0.0.1"; // at least one starting point to talk
	public CassandraHosts() {

	}

	public static String getHost() {
		return (Host);
	}

	public static String[] getHosts(Cluster cluster) {

		if (cluster == null) {
			System.out.println("Creating cluster connection");
			cluster = Cluster.builder().addContactPoint(Host).build();
		}
		System.out.println("Cluster Name " + cluster.getClusterName());
		Metadata mdata = cluster.getMetadata();
		Set<Host> hosts = mdata.getAllHosts();
		String sHosts[] = new String[hosts.size()];

		Iterator<Host> it = hosts.iterator();
		int i = 0;
		while (it.hasNext()) {
			Host ch = it.next();
			sHosts[i] = (String) ch.getAddress().toString();

			System.out.println("Hosts" + ch.getAddress().toString());
			i++;
		}

		return sHosts;
	}

	public static Cluster getCluster(){
		System.out.println("getCluster");
		try {
			cluster = Cluster.builder().addContactPoints("192.168.2.10").build(); // vagrant
			cluster.init();	//Force an initial connection  which will throw a no host available exception if vagrant isn't there																	// cassandra
                        Host="192.168.2.10";

		} catch (NoHostAvailableException | AuthenticationException | IllegalStateException et) {
			System.out.println("No Vagrant host " + et);
			try {
				cluster = Cluster.builder().addContactPoint("127.0.0.1").build(); // localhost
				cluster.init();
                                Host="127.0.0.1";
			} catch (NoHostAvailableException | AuthenticationException | IllegalStateException et1) {
				// can't get to a cassandra cluster bug out
                                Host="0.0.0.0";
				return null;

			}
		}

		Keyspaces.SetUpKeySpaces(cluster);

		return cluster;

	}

	public void close() {
		cluster.close();
	}

}
