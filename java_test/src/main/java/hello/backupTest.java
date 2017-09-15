package hello;

import com.thinkaurelius.titan.core.TitanFactory;  
import com.thinkaurelius.titan.core.TitanGraph;  
import com.thinkaurelius.titan.core.attribute.Contain;  
import com.thinkaurelius.titan.core.attribute.Geoshape;  
import com.thinkaurelius.titan.core.attribute.Text;  
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
  
import java.io.File;  
import java.util.Iterator;  
import java.util.Map;  

// public class HelloWorld{
//  public static void main(String []args)
//  {
//      System.out.println("Hello World");
//  }
// }

public class TestTitan {  
    public static final String INDEX_NAME = "search";  
  
    public static void main(String[] args) {  
    System.out.println("Hellow Titan");

        TitanFactory.Builder config = TitanFactory.build();  
        config.set("storage.backend", "hbase");  
        config.set("storage.hostname", "localhost");  
        //config.set("index." + INDEX_NAME + ".backend", "elasticsearch");  

        //这里的这个/tmp/titan 是本地的路径,需要先创建文件夹  
        //config.set("index." + INDEX_NAME + ".directory", "/tmp/titan" + File.separator + "es");  
        //config.set("index." + INDEX_NAME + ".elasticsearch.local-mode", true);  
        //config.set("index." + INDEX_NAME + ".elasticsearch.client-only", false);  
        TitanGraph graph = config.open();  
          
        GraphTraversalSource g = graph.traversal();  
        //第一次需要先load这个  将数据导入到本地的hbase中  


        //下面的例子可以结合刚才文档中chapter 3 中的例子看  
        //GraphOfTheGodsFactory.load(g);  
        //对应gremlin内的 g.V().has('name','hercules').next().value('name')  
        System.out.println("======" + g.V().has("name", "hercules").next().value("name"));  
        
        //g.V().has('name','hercules').next().values('name','age')  
        System.out.println("======" + g.V().has("name", "hercules").next().values("name", "age"));  
        Iterator iterator = g.V().has("name","hercules").next().values("name", "age");  
        while(iterator.hasNext()){  
            Object o  = iterator.next();  
            System.out.println("======"+o);  
        }  
  
        Vertex saturn = g.V().has("name","saturn").next();  
  
        System.out.println("======"+saturn);  
  
        //g.V(vertex).in("father").in("father").next()  得到 saturn的孙子节点  
        System.out.println("======" + g.V(saturn).in("father").in("father").next().value("age"));  
  
        GraphTraversal<Edge, Edge> a =  g.E().has("place", P.eq(Geoshape.point(38.1f, 23.7f)));  
        System.out.println("======"+a);  
        while(a.hasNext()){  
            Edge e = a.next();  
            System.out.println("======"+e.keys());  
            System.out.println("======"+e.label());  
            System.out.println("======"+e.outVertex().value("name"));  
            System.out.println("======"+e.inVertex().value("name"));  
            System.out.println("======"+e.value("time")+"  :  "+e.value("place"));  
        }  
  
        Vertex hercules = g.V().has("name","hercules").next();  
        System.out.println("======"+g.V(hercules).out("mother","father").values("name"));  
        GraphTraversal<Vertex,Vertex> mF = g.V(hercules).out("mother", "father");  
        while(mF.hasNext()){  
            Vertex v = mF.next();  
            System.out.println("======"+ v.label()+"  :  "+v.value("name"));  
        }  
  
        System.out.println("======" + g.V(saturn).repeat(__.in("father")).times(2).next().value("name"));  
  
        GraphTraversal<Vertex,Vertex> monsters = g.V(hercules).out("battled");  
        while(monsters.hasNext()){  
            Vertex monster = monsters.next();  
            System.out.println("======"+monster.label()+"  :  "+monster.value("name"));  
        }  
  
        monsters = g.V(hercules).outE("battled").has("time",P.eq(1)).inV();  
        while(monsters.hasNext()){  
            Vertex monster = monsters.next();  
            System.out.println("======"+monster.label()+"  :  "+monster.value("name"));  
        }  
        Vertex pluto = g.V().has("name","pluto").next();  
        //通过out得到住的地方的节点,在in得到所有链接到这个地方的节点,从而得到所有住在这个地方的节点  out 边出去的条件  in  边进来的条件  
  
        GraphTraversal<Vertex,Vertex> liveInTartarusVertex = g.V(pluto).out("lives").in("lives");  
        while(liveInTartarusVertex.hasNext()){  
            Vertex vertex = liveInTartarusVertex.next();  
            System.out.println("======"+vertex.value("name"));  
        }  
  
  
  
        GraphTraversal<Vertex,Vertex> liveInTartarusVertexNo = g.V(pluto).out("lives").in("lives").where(  
                __.is(P.neq(pluto)));  
        while(liveInTartarusVertexNo.hasNext()){  
            Vertex vertex = liveInTartarusVertexNo.next();  
            System.out.println("======"+vertex.value("name"));  
        }  
  
  
        GraphTraversal<Vertex,Vertex> liveInTartarusVertexNot = g.V(pluto).as("x").out("lives").in("lives").where(P.neq(  
                "x"));  
        while(liveInTartarusVertexNot.hasNext()){  
            Vertex vertex = liveInTartarusVertexNot.next();  
            System.out.println("======"+vertex.value("name"));  
        }  
  
        GraphTraversal<Vertex,Map<String, Vertex>> brothers = g.V(pluto).out("brother").as("god").out("lives").as("place").select("god","place");  
        while(brothers.hasNext()){  
            Map<String,Vertex> map = brothers.next();  
            System.out.println("======"+map);  
            for(Map.Entry<String,Vertex> entry:map.entrySet()){  
                System.out.println(entry.getKey()+" : "+entry.getValue().value("name"));  
            }  
        }  
  
        System.out.println("======"+g.V(pluto).outE("lives").next().value("reason"));  
  
  
        /*GraphTraversal<Edge,Object> reasons = g.E().has("reason").values("reason").is(Text.textContains("loves")); 
        System.out.println(reasons); 
        while(reasons.hasNext()){ 
            Object e = reasons.next(); 
            System.out.println("======"+e); 
 
        }*/  
  
        GraphTraversal<Edge,Edge> reasons = g.E().has("reason").as("r").values("reason").is(Text.textContains("loves")).select("r");  
        System.out.println(reasons);  
        while(reasons.hasNext()){  
            Edge e = reasons.next();  
            System.out.println("======"+e.keys());  
            System.out.println("======"+e.label());  
            System.out.println("======"+e.value("reason"));  
        }  
  
        GraphTraversal<Edge,Map<String,Object>> reasons2 = g.E().has("reason").as("source").values("reason").is(Text.textContains("loves")).as("reason").select("source")  
                .outV().values("name").as("god").select("source").inV().values("name").as("thing").select("god","reason","thing");  
  
        while(reasons2.hasNext()){  
            Map<String,Object> map = reasons2.next();  
            System.out.println("======"+map);  
            for(Map.Entry<String,Object> entry:map.entrySet()){  
                System.out.println(entry.getKey()+" :  "+entry.getValue());  
            }  
        }  
        System.out.println("======");  
  
//        System.out.println("======"+g.V(pluto).out("lives").in("lives"));  
        graph.close();  
    }  
} 
