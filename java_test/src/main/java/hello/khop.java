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


///////
class KHopDemon implements Runnable {
    public Thread _t;
    public Integer _tedges;

    private String _threadName;
    private GraphTraversalSource _g;
    private Vertex [] _root;
    private Integer _k;

    KHopDemon (String name, GraphTraversalSource g, Vertex [] root, Integer k, Integer tedges) {
        _threadName = name;
        _g = g;
        _root = root;
        _k = k;
        _tedges = tedges;
    }

    public void run(){
        System.out.println("====== Running " + _threadName);

        int num_runs = _root.length;

        long time_min = Long.MAX_VALUE, time_max = 0, time_mean = 0;
        long start_t = 0, end_t = 0;
        long [] time_all = new long [num_runs];

        for (int i = 0; i < _root.length; i ++)
        {
            start_t = System.currentTimeMillis();

            Integer tmp_tedge = k_hop(_g, _root[i], _k);

            _tedges += tmp_tedge;

            end_t = System.currentTimeMillis();

            time_mean += end_t - start_t;
            time_all[i] = end_t - start_t;
            time_min = Math.min(time_min, end_t - start_t);
            time_max = Math.max(time_max, end_t - start_t);
        }

        try{
            File file = new File("./test_titan.log");

            if (!file.exists()){
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);

            BufferedWriter bw = new BufferedWriter(fw);

            // System.out.println(
            synchronized(this){
                bw.write(
                                       "====== Execution time" +
                                       ", Min: " + time_min + 
                                       ", Max: " + time_max +
                                       ", Mean: " + time_mean/num_runs + 
                                       ", All: " );

                for (int i = 0; i <  num_runs; i ++) {
                    bw.write(
                                time_all[i] + " "
                                );
                }

                bw.write (
                           " ms\n" );
    
                //System.out.println(
                bw.write(
                                    "====== "+_threadName+": num of travered edges per time "+ ("" + _tedges/num_runs) +"\n" );  
    
                //System.out.println("======");  
            }

            bw.close();

        } catch (IOException e){
            e.printStackTrace();
        }
        
    
        
    }

    public void start(){
        if (_t == null){
            _t = new Thread (this, _threadName);
            _t.start();
        }
    }

    /*K hop traversal test*/
    public static Integer k_hop(GraphTraversalSource g, Vertex root, Integer k) {

        Queue<Vertex> deque_v = new LinkedList<Vertex>();
        Queue<Integer> deque_depth = new LinkedList<Integer>();

        Set<Vertex> visited = new HashSet<Vertex>();

        Integer k_int = k;
        Integer init_depth = 0;
        Integer tedges = 0;

        deque_v.add(root);
        deque_depth.add(init_depth);

        visited.add(root);

        while (!deque_v.isEmpty())
        {
            Vertex cur_v = (Vertex) deque_v.remove();

            Integer cur_depth = (Integer) deque_depth.remove();

            if(cur_depth < k_int)
            {
                cur_depth++;

                GraphTraversal<Vertex,Vertex> out_edges = g.V(cur_v).out("connect"); 

                while(out_edges.hasNext()){  

                    Vertex vertex = out_edges.next();  

                    if(!visited.contains(vertex))
                    {
                        deque_v.add(vertex);
                        deque_depth.add(cur_depth);

                        visited.add(vertex);

                        tedges += 1;
                    }
                }  
            }
        }

        return tedges;

    }

}


