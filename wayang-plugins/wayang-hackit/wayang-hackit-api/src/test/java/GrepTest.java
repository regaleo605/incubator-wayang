import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.plugin.hackit.core.tags.DebugTag;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.spark.Spark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

public class GrepTest {
    public static void main(String[] args) {
        WayangContext wayang = new WayangContext().withPlugin(Java.basicPlugin());
        HackItDataQuanta<String> hackit3 = new HackItDataQuanta<>(wayang);
        HackitTag debug = new DebugTag();
        long t = System.currentTimeMillis();
        Collection<HackitTuple> flat = hackit3.loadTextFile(FILE_SOME_LINES_TXT.toString(),true)
                .flatMap(x-> Arrays.asList(x.split(" ")),String.class,String.class,null
                        ,null)
                .map(x->new Tuple2<>(x,1),null,String.class, Tuple2.class,null,null)
                .reduceBy(x->x.getField0(),(a,b)->new Tuple2<>(a.getField0().toLowerCase(),a.getField1()+b.getField1())
                        ,String.class,Tuple2.class, null,null)
                //.filter(x->x.getField1() > 10000,Tuple2.class,null,x->{x.addPostTag(debug);return x;})
                .collect(Tuple2.class);
        long finish = System.currentTimeMillis();
        System.out.println(flat.size());
        System.out.println(finish-t);
    }

    public static final URI FILE_SOME_LINES_TXT = createUri("/some-lines.txt");

    //public static final URI TEST_TXT = createUri("/long-abstracts.txt");

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }
    }
}
