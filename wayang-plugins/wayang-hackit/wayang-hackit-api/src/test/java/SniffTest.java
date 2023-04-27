import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.plugin.hackit.core.tags.*;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.spark.Spark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

public class SniffTest {

    public static void main(String[] args) {
        long t = System.currentTimeMillis();
        WayangContext wayang = new WayangContext().withPlugin(Spark.basicPlugin());
        HackItDataQuanta<String> hackit3 = new HackItDataQuanta<>(wayang);
        HackitTag debug = new DebugTag();
        Collection<HackitTuple> flat = hackit3.loadTextFile(TEST_TXT.toString(),null)
                .filter(x-> x.contains("some"),String.class,null
                        ,x->{x.addPostTag(new DisplayTag());return x;})
                .sniff(String.class)
                .collect(String.class);
        /*
        Collection<HackitTuple> flat = hackit3.loadTextFile(TEST_TXT.toString(),true)
                .flatMap(x-> Arrays.asList(x.split(" ")),String.class,String.class,null
                        ,x->{x.addPostTag(new DisplayTag());return x;})
                .map(x->new Tuple2<>(x,1),null,String.class, Tuple2.class,null,null)
                .reduceBy(x->x.getField0(),(a,b)->new Tuple2<>(a.getField0().toLowerCase(),a.getField1()+b.getField1())
                        ,String.class,Tuple2.class, null,x->{if(x.getValue().getField1()>2) x.addPostTag(new SkipTag());return x;})
                .collect(Tuple2.class);
         */
        long finish = System.currentTimeMillis();
        System.out.println(flat.size());
        System.out.println(finish-t);
        flat.forEach(x-> {
            System.out.println(x);
            System.out.println(x.getPostTag());
        });
    }

    public static final URI TEST_TXT = createUri("/some-lines.txt");
    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }
    }
}
