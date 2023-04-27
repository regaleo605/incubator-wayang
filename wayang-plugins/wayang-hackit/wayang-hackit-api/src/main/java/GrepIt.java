import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.plugin.hackit.core.tags.DebugTag;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.spark.Spark;

import java.net.URI;
import java.net.URISyntaxException;

public class GrepIt {
    public static void main(String[] args) {
        WayangContext wayang = new WayangContext();
        wayang.register(Spark.basicPlugin());
        HackItDataQuanta<String> hackit3 = new HackItDataQuanta<>(wayang);
        HackitTag debug = new DebugTag();
        long t = System.currentTimeMillis();
        /*Collection<HackitTuple> flat = hackit3.loadTextFile(TEST_TXT.toString(),true)
                .filter(x->x.contains("some"),String.class,null,null)//.filter(x->x.getField1() > 10000,Tuple2.class,null,x->{x.addPostTag(debug);return x;})
                .collect(String.class);

         */
        hackit3.loadTextFile(TEST_TXT.toString(),true)
                .sniff(String.class)
                .filter(x->x.contains("some"),String.class,x->{x.addPreTag(debug); return x;},null)
                .sniff(String.class)//.filter(x->x.getField1() > 10000,Tuple2.class,null,x->{x.addPostTag(debug);return x;})
                .collect(String.class);
        long finish = System.currentTimeMillis();
        //System.out.println(flat.size());
        System.out.println(finish-t);
    }
    public static final URI TEST_TXT = createUri("/ex-lines.txt");
    public static final URI WRITE_TXT = createUri("/lala.txt");

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }
    }

}
