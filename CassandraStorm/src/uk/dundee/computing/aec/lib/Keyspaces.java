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
			String createkeyspace="CREATE KEYSPACE if not exists stormspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};";
			String CreateSyncTable = "CREATE TABLE stormspace.stormsync ("+
					"minute varchar,"+
					"processtime varchar,"+
					"interaction_time timeuuid,"+
					"Value varchar,"+
					"SaverId varChar,"+
					"host varChar,"+
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
				System.out.println("Can't create keyspace2 "+et);
			}

			System.out.println(""+CreateSyncTable);
            session.close();
            session =c.connect("stormspace");
			try{
				SimpleStatement cqlQuery = new SimpleStatement(CreateSyncTable);
				session.execute(cqlQuery);
			}catch(Exception et){
				System.out.println("Can't create StormSync table "+et);
			}

			session.close();

		}catch(Exception et){
			System.out.println("Other keyspace or column definition error" +et);
		}

	}
}
