package hello;

import com.thinkaurelius.titan.core.TitanFactory;  
import com.thinkaurelius.titan.core.TitanGraph;  
import com.thinkaurelius.titan.core.TitanVertex;  
import com.thinkaurelius.titan.core.attribute.Contain;  
import com.thinkaurelius.titan.core.attribute.Geoshape;  
import com.thinkaurelius.titan.core.attribute.Text;  
import com.thinkaurelius.titan.core.TitanTransaction;  
import com.thinkaurelius.titan.core.attribute.Geoshape;  
import com.thinkaurelius.titan.core.attribute.Text;  
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;  
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;  
import com.thinkaurelius.titan.core.schema.TitanManagement;  
import com.thinkaurelius.titan.core.EdgeLabel;  
import com.thinkaurelius.titan.core.Multiplicity;  
import com.thinkaurelius.titan.core.PropertyKey;  
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;  

import org.apache.tinkerpop.gremlin.process.traversal.Contains;  
import org.apache.tinkerpop.gremlin.process.traversal.P;  
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;  
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;  
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;  
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;  
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;  
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;  
import org.apache.tinkerpop.gremlin.structure.Edge;  
import org.apache.tinkerpop.gremlin.structure.Vertex;  
import org.apache.tinkerpop.gremlin.structure.Direction;  
import org.apache.tinkerpop.gremlin.structure.T;  
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

// import com.tinkerpop.blueprints.Edge;
// import com.tinkerpop.blueprints.Vertex;
 
import java.io.*; 
import java.io.File; 
import java.io.BufferedReader;
import java.io.FileReader; 
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.stream.Stream;
import java.util.*;
import java.util.Iterator;  
import java.util.Map;  
import java.lang.Math;

import hello.PageRank;
import hello.KHopDemon;

//Class: TestTitan
public class testTitan {  
    /*Test k hop traversal*/
    public static void test_khop(String[] args) throws FileNotFoundException {  

        System.out.println("Hellow Titan");
        System.out.println("====== Start loading");

        long start_t = System.currentTimeMillis(); 

        //Open graph in hbase
        TitanFactory.Builder config = TitanFactory.build();  

        config.set("storage.backend", "hbase");  
        config.set("storage.hostname", "localhost");  

        TitanGraph graph = config.open();  

        GraphTraversalSource g = graph.traversal(); 

        //Drop all the existing vertices
        //g.V().drop().iterate();
        long end_t = System.currentTimeMillis();

        //System.out.println("====== num of vertex:" + IteratorUtils.count(g.V()));  
        //System.out.println("====== num of edges: " + IteratorUtils.count(g.E()));  
        long load_time = end_t - start_t;
        // System.out.println("====== Total data load time: " + (end_t - start_t) );
        
        /////////////////////
        Integer k = 3;

        int num_thread = 100, step = 0, num_runs = 100;

        GraphTraversal<Vertex,Vertex> vertices = g.V(); 

        List<KHopDemon> queries = new ArrayList<KHopDemon>();

        Integer [] tedges = new Integer[num_thread];

        for (int i = 0; i < num_thread; i++)
            tedges[i] = 0;
    
        start_t = System.currentTimeMillis();

        for (int i = 0; i < num_thread; i ++){
            int offset = 0;
    
            Vertex [] root_list = new Vertex [num_runs];

            for (int j = 0; j < num_runs; j++) {

                Random rand = new Random();
    
                step = rand.nextInt(100) + 1;

                Vertex root = vertices.next(); 
        
                while (vertices.hasNext() && offset < step)
                {
                    root = vertices.next(); 

                    offset++;
                }

                root_list[j] = root;

                if (!vertices.hasNext()){
                    vertices = g.V();
                }

            }

            // System.out.println("====== Root value: " + root.value("username") );

            KHopDemon q = new KHopDemon("q" + ("" + i), g, root_list, k, tedges[i]);
            
            q.start();

            queries.add(q);
        }

        Integer sum = 0;

        try {
            for (KHopDemon x: queries){
                x._t.join();
                sum += x._tedges;
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    
        end_t = System.currentTimeMillis();



        ///Print out results
        try{
            File file = new File("./test_titan_khop.log");

            if (!file.exists()){
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);

            BufferedWriter bw = new BufferedWriter(fw);

            // System.out.println(
            bw.write(
                      "====== Sum of ave run time of each query: " + (end_t - start_t)/num_thread + " ms\n" );
            bw.write(
                      "====== Sum of travered edges for each query: " + ("" + sum/num_thread) + "\n");  
            bw.write(
                      "====== Total data load time: " + (load_time) + " ms\n" );
            bw.close();

            //System.out.println("======");  
        } catch (IOException e){
            e.printStackTrace();
        }
  
        graph.close();  
    }  


    /*Test pagerank*/
    public static void main(String[] args) throws FileNotFoundException {  

        System.out.println("Hellow Titan");
        System.out.println("====== Start loading");

        long start_t = System.currentTimeMillis(); 

        //Open graph in hbase
        TitanFactory.Builder config = TitanFactory.build();  

        config.set("storage.backend", "hbase");  
        config.set("storage.hostname", "localhost");  

        TitanGraph graph = config.open();  

        GraphTraversalSource g = graph.traversal(); 

        //Drop all the existing vertices
        //g.V().drop().iterate();
        long end_t = System.currentTimeMillis();

        //System.out.println("====== num of vertex:" + IteratorUtils.count(g.V()));  
        //System.out.println("====== num of edges: " + IteratorUtils.count(g.E()));  
        long load_time = end_t - start_t;
        // System.out.println("====== Total data load time: " + (end_t - start_t) );
        
        /////////////////////\
        int num_thread = 1, num_runs = 1;

        double damping_factor = 0.85d, vertex_count = 0.0d;

        int num_iterations = 30;

        GraphTraversal<Vertex,Vertex> vertices = g.V(); 

        while (vertices.hasNext()){
            vertex_count ++;
            vertices.next();
        }

        System.out.println("Total num of vertices: " + (vertex_count));

        //vertices = g.V();

        List<PageRank> queries = new ArrayList<PageRank>();
    
        start_t = System.currentTimeMillis();

        for (int i = 0; i < num_thread; i ++){

            PageRank q = new PageRank("q" + ("" + i), g, damping_factor, num_iterations, vertex_count);
            
            q.start();

            queries.add(q);
        }

        try {
            for (PageRank x: queries){
                x._t.join();
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    
        end_t = System.currentTimeMillis();



        ///Print out results
        try{
            File file = new File("./test_titan_pagerank.log");

            if (!file.exists()){
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);

            BufferedWriter bw = new BufferedWriter(fw);

            // System.out.println(
            bw.write(
                      "====== Sum of ave run time of each query: " + (end_t - start_t)/num_thread + " ms\n" );
            bw.write(
                      "====== Total data load time: " + (load_time) + " ms\n" );
            bw.close();

            //System.out.println("======");  
        } catch (IOException e){
            e.printStackTrace();
        }
  
        graph.close();  
    }  

    /*Load graph*/
    public static void load_graph(GraphTraversalSource g) throws FileNotFoundException  {
        Map<String, Vertex> map = new HashMap<String, Vertex>();

        try{
            String FilePath = "./src/main/java/hello/orkut.txt";
            InputStreamReader isr = new InputStreamReader(new FileInputStream(FilePath));
            //File e_file = new File("./src/main/java/hello/orkut.txt");

            System.out.println("====== Opened file");  
            BufferedReader br_e = new BufferedReader(isr);
            String line;
            int cnt_e = 0, cnt_v = 0;
    
            while((line = br_e.readLine()) != null)
            {
                cnt_e++;

                if (cnt_e <= 77800000)
                    continue;

                String src, dest;
                String label = "connect";
                //System.out.println("====== line " + line); 

                String [] ids = line.split("\\s+");
                if (ids.length != 2){
                    for (String x: ids)
                        System.out.println(x + "-");
                    break;
                }

                src = ids[0]; 
                dest = ids[1];

                //System.out.println("====== src: " + src + " dest: " + dest); 

                Vertex v_src, v_dest;

                if (map.containsKey(src)) {
                    //v_src = g.V().has("username", src).next();
                    v_src = map.get(src);
                    //System.out.println("====== Has source"); 
                } else {
                    if (g.V().has("username", src).hasNext())
                    {
                        v_src = g.V().has("username", src).next();
                    } else 
                    {
                        v_src = g.addV("username", src).next();
                        cnt_v++;
                    }
                    map.put(src, v_src);
                    //System.out.println("====== Vertex src username: " + v_src.value("username")); 
                }

                //if (g.V().has("username", dest).hasNext()) {
                if (map.containsKey(dest)) {
                    //v_dest = g.V().has("username", dest).next();
                    v_dest = map.get(dest);
                    //System.out.println("====== Has dest"); 
                } else 
                {
                    if (g.V().has("username", dest).hasNext()) 
                    {
                        v_dest = g.V().has("username", dest).next();
                    } else
                    {
                        v_dest = g.addV("username", dest).next();   
                    }
                    map.put(dest, v_dest);
                    //System.out.println("====== Vertex src username: " + v_dest.value("username")); 
                    //v_dest.property(, dest);
                }
                
                v_src.addEdge("connect", v_dest);
                //v_dest.addEdge("connect", v_src);

                if (cnt_e % 200000 == 0){
                    System.out.println("====== added 200000 edge, added " + cnt_e + " total edges"); 
                    g.tx().commit();
                }

                //commit all the changes
                g.tx().commit();
        
                //Get the traversal data of graph
                //g = graph.traversal();  
                System.out.println("====== added num of vertex: " + cnt_v);  
                System.out.println("====== added num of edges: " + cnt_e);  

            }
        } catch (IOException e){
            return;
        }

    }

    
} 

