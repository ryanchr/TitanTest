import org.apache.commons.configuration.BaseConfiguration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class TitanDemo {
    public static void main(String args[]) {
        BaseConfiguration baseConfiguration = new BaseConfiguration();

        baseConfiguration.setProperty("storage.backend", "hbase");
        baseConfiguration.setProperty("storage.hostname", "192.168.0.150");
        baseConfiguration.setProperty("storage.tablename","test");

        TitanGraph titanGraph = TitanFactory.open(baseConfiguration);
        Vertex rash = titanGraph.addVertex(null);

        rash.setProperty("userId", 1);
        rash.setProperty("username", "rash");
        rash.setProperty("firstName", "Rahul");
        rash.setProperty("lastName", "Chaudhary");
        rash.setProperty("birthday", 101);

        Vertex honey = titanGraph.addVertex(null);

        honey.setProperty("userId", 2);
        honey.setProperty("username", "honey");
        honey.setProperty("firstName", "Honey");
        honey.setProperty("lastName", "Anant");
        honey.setProperty("birthday", 201);
        
        Edge frnd = titanGraph.addEdge(null, rash, honey, "FRIEND");
        frnd.setProperty("since", 2011);
        titanGraph.commit();
        
        Iterable<Vertex> results = rash.query().labels("FRIEND")
                .has("since", 2011).vertices();
        for (Vertex result : results) {
            System.out.println("Id: " + result.getProperty("userId"));
            System.out.println("Username: " + result.getProperty("username"));
            System.out.println("Name: " + result.getProperty("firstName") + " "
                    + result.getProperty("lastName"));
        }
        titanGraph.shutdown();
    }
}