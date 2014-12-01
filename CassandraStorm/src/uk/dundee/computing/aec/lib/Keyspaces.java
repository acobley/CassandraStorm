package uk.dundee.computing.aec.lib;


import java.util.ArrayList;
import java.util.List;







import com.datastax.driver.core.*;

public final class Keyspaces {



	public Keyspaces(){

	}

	public static void SetUpKeySpaces(Cluster c){
		try{
			//Add some keyspaces here
			String createkeyspace="CREATE KEYSPACE if not existskeyspace2 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};";
			String CreateSyncTable = "CREATE TABLE StormSync ("+
					"minute varchar,"+
					"processtime varchar,"+
					"interaction_time timeuuid,"+
					"Value varchar,"+
					"SaverId varChar,"+
					"PRIMARY KEY (minute,interaction_time)"+
					") with CLUSTERING ORDER BY (interaction_time DESC);";

			Session session = c.connect();
			try{
				PreparedStatement statement = session
						.prepare(createkeyspace);
				BoundStatement boundStatement = new BoundStatement(
						statement);
				ResultSet rs = session
						.execute(boundStatement);

			}catch(Exception et){
				System.out.println("Can't create mzMLKeyspace "+et);
			}

			//now add some column families 
			session.close();
			session = c.connect("mzMLKeyspace");
			System.out.println(""+CreateSyncTable);

			try{
				SimpleStatement cqlQuery = new SimpleStatement(CreateSyncTable);
				session.execute(cqlQuery);
			}catch(Exception et){
				System.out.println("Can't create tweet table "+et);
			}

			session.close();

		}catch(Exception et){
			System.out.println("Other keyspace or column definition error" +et);
		}

	}
}
