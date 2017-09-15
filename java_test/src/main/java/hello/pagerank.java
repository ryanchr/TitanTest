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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
/*For pagerank com*/
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;

import org.apache.tinkerpop.gremlin.structure.Edge;  
import org.apache.tinkerpop.gremlin.structure.Vertex;  
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.Direction;  
import org.apache.tinkerpop.gremlin.structure.T;  
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.configuration.Configuration;
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
import java.util.Set;

///////
class PageRank implements Runnable {
    /*THreads*/
    public Thread _t;

    private String _threadName;
    private GraphTraversalSource _g;

    /* Pagerank values */
    private Double _dampingFactor;
    private Integer _maxIterations;
    private Double _vertexCount;

    PageRank (String name
            , GraphTraversalSource g
            , Double dampingFactor
            , Integer maxIterations
            , Double vertexCount) {

        _threadName = name;
        _g = g;

        /* Initialize Pagerank parameter values */
        _dampingFactor = dampingFactor;
        _maxIterations = maxIterations;
        _vertexCount = vertexCount;
    }

    public void run(){
        System.out.println("====== Running " + _threadName);

        int num_runs = 1;

        long time_min = Long.MAX_VALUE, time_max = 0, time_mean = 0;
        long start_t = 0, end_t = 0;
        long [] time_all = new long [num_runs];

        for (int i = 0; i < num_runs; i ++)
        {
            start_t = System.currentTimeMillis();

            /*Initialize all property values*/
            pagerank_init(_g);

            /*Run pagerank one time*/
            pagerank_run(_g);

            end_t = System.currentTimeMillis();

            /*Get execution time statistics*/
            time_mean += end_t - start_t;
            time_all[i] = end_t - start_t;
            time_min = Math.min(time_min, end_t - start_t);
            time_max = Math.max(time_max, end_t - start_t);
        }

        try{
            File file = new File("./test_titan_pagerank.log");

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


    /*Page rank initialization*/
    public void pagerank_init(GraphTraversalSource g) {

        GraphTraversal<Vertex,Vertex> vertices = g.V(); 

        while ( vertices.hasNext() ){
            Vertex cur_v = vertices.next();

            /* Add the new property of pagerank value */
            cur_v.<Double>property(VertexProperty.Cardinality.single, "PRVal", 1.0D);

            GraphTraversal<Vertex,Vertex> out_edges = g.V(cur_v).out("connect"); 

            /*Get the number of outgoing edges for a vertex*/
            Double num_out_e = 0.0;

            while (out_edges.hasNext()){

                out_edges.next();

                num_out_e += 1;

            }

            /*Set up the num of outgoing edges*/
            cur_v.<Double>property(VertexProperty.Cardinality.single, "NumOutEdges", num_out_e);

        }

        return;

    }

    /*Pagerank test*/
    public void pagerank_run(GraphTraversalSource g) {
    
        /*Parameters*/
        Double alpha = this._dampingFactor;

        int totalIterations = 30;
    
        /*Initialize page rank values*/
        for(int i = 0; i < totalIterations; i++){

            GraphTraversal<Vertex,Vertex> vertices = g.V(); 

            while ( vertices.hasNext() ){
                Vertex cur_v = vertices.next();
    
                /* upddate the pagerank value */
                GraphTraversal<Vertex, Vertex> out_edges = g.V(cur_v).out("connect"); 

                Double page_rank_v =  ((1.0D - alpha) / _vertexCount);

                while (out_edges.hasNext()){
                    Vertex out_v = out_edges.next();

                    page_rank_v += out_v.<Double>value("PRVal") * alpha / out_v.<Double>value("NumOutEdges");
                }

                cur_v.<Double>property("PRVal", page_rank_v);
    
            }

        }


    }

}


