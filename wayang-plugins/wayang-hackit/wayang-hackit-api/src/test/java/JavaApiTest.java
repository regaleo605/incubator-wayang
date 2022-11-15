import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.plugin.hackit.core.tags.*;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.spark.Spark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class JavaApiTest {
    public static void main(String[] args) {

        WayangContext wayang = new WayangContext().with(Spark.basicPlugin());
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        List<HackitTuple> data2 = Arrays.asList(new HackitTuple(1),new HackitTuple(2),new HackitTuple(3));
        data2.get(1).addPreTag(new DebugTag());
        data2.get(1).addPreTag(new PauseTag());

        HackItDataQuanta<HackitTuple> hackIt = new HackItDataQuanta(wayang);

        HackitTag debug = new DebugTag();
        HackitTag pause = new PauseTag();
        HackitTag log = new LogTag();
        HackitTag skip = new SkipTag();
        Set<HackitTag> set = new HashSet<>();
        set.add(debug);set.add(pause);

        /*
       HackItDataQuanta<Integer> hackitInt = new HackItDataQuanta<>(wayang);
       Collection<HackitTuple> result = hackitInt.loadCollection(data2, HackitTuple.class).
               map(x->x*2,null).sniff()
                       .map(x->x*2,null).sniff().
               collect(HackitTuple.class);

        System.out.println(result);

         */
/*
        Collection<HackitTuple> result = hackitInt.loadCollection(data2,HackitTuple.class)
                .map(x->x*2,null
                        ,x->{
                    if(x.getValue() %2==0) x.addPreTag(skip);
                    return x;}
                ,null)
                .filter(x->x%3==1,x->{
                    x.addPreTag(debug);
                    return x;
                },x->{
                    x.addPostTag(skip);
                    return x;
                })
                .collect(HackitTuple.class);
        System.out.println(result);
        result.forEach(x->{
            System.out.println(x.getHeader().getPreTags());
            System.out.println(x.getHeader().getPostTags());
        });


 */

        HackItDataQuanta<String> hackit3 = new HackItDataQuanta<>(wayang);
        long t = System.currentTimeMillis();
        Collection<HackitTuple> flat = hackit3.loadTextFile(FILE_SOME_LINES_TXT.toString(),true)
                .flatMap(x-> Arrays.asList(x.split(" ")),String.class,String.class,null
                        ,null)
                .map(x->new Tuple2<>(x,1),null,String.class, Tuple2.class,null,null)
                .reduceBy(x->x.getField0(),(a,b)->new Tuple2<>(a.getField0().toLowerCase(),a.getField1()+b.getField1())
                        ,String.class,Tuple2.class, null,null)
                .filter(x->x.getField1() > 10000,Tuple2.class,null,null)
                .collect(Tuple2.class);
        long finish = System.currentTimeMillis();
        System.out.println(flat.size());
        System.out.println(finish-t);




/*
//FlatMap Example
        HackItDataQuanta<String> hackit3 = new HackItDataQuanta<>(wayang);
        Collection<HackitTuple> flat = hackit3.
                loadTextFile(FILE_SOME_LINES_TXT.toString(),true)
                .flatMapHackit(line -> Arrays.asList(line.split(" ")),
                String.class,
                String.class,set,set)
                //.mapNormal(word -> new Tuple2(word.toLowerCase(),1),null,String.class,Tuple2.class)
                        .collect(String.class);

        System.out.println(flat);
 */





/*
        Collection<HackitTuple> result = hackitInt.loadCollection(data2,HackitTuple.class)
                .map(x->x*2,null)
                .reduceHackit((a,b) -> a + b,debug,debug)
                .collect(HackitTuple.class);

        System.out.println(result);


 */





        //Join Example
/*
Set<HackitTag> tags = new HashSet<>();
tags.add(skip);
        List<Tuple2<Integer,String>> tup1 = Arrays.asList(new Tuple2<>(1,"a"),new Tuple2<>(2,"b"),new Tuple2<>(3,"c"));
        List<Tuple2<Integer,String>> tup2 = Arrays.asList(new Tuple2<>(1,"d"),new Tuple2<>(2,"e"),new Tuple2<>(3,"f"));

        List<HackitTuple<Object,Tuple2<Integer,String>>> htup1 = Arrays.asList(new HackitTuple<>(new Tuple2<>(1,"a")),
                                        new HackitTuple<>(new Tuple2<>(2,"b")),new HackitTuple<>(new Tuple2<>(3,"c")));
        List<HackitTuple<Object,Tuple2<Integer,String>>> htup2 = Arrays.asList(new HackitTuple<>(new Tuple2<>(1,"d")),
                new HackitTuple<>(new Tuple2<>(2,"e")),new HackitTuple<>(new Tuple2<>(3,"f")));
          HackItDataQuanta<Tuple2> hackitjoin = new HackItDataQuanta<>(wayang).loadCollectionTuple(htup1,HackitTuple.class);
        HackItDataQuanta<Tuple2> hackitjoin2 = new HackItDataQuanta<>(wayang).loadCollectionTuple(htup2,HackitTuple.class);

        Collection<HackitTuple> result = hackitjoin.joinHackit(hackitjoin2,x->x.getField0(),x->x.getField0(),Integer.class,tags,tags).collect(Tuple2.class);
        System.out.println(result);
        result.forEach(x->{
            System.out.println(x.getPreTag());
            System.out.println(x.getPostTag());
        });

 */

        //normal

/*
        HackItDataQuanta<Tuple2> hackitjoin = new HackItDataQuanta<>(wayang).loadCollectionTuple(tup1,Tuple2.class);
        HackItDataQuanta<Tuple2> hackitjoin2 = new HackItDataQuanta<>(wayang).loadCollectionTuple(tup2,Tuple2.class);

        Collection result = hackitjoin.join(hackitjoin2,x->x.getField0(),x->x.getField0(),Tuple2.class,Tuple2.class,Integer.class).collect(Tuple2.class);
        System.out.println(result);

         */




        /*
        Repeat Example
        HackItDataQuanta<Integer> normal = new HackItDataQuanta<>(wayang);
        Collection result = normal.loadCollection(data, Integer.class).
                repeat(2,Integer.class, iteration ->
                        iteration.mapNormal(x->x*2,null,Integer.class,Integer.class)).collect(Integer.class);

        System.out.println(result);
         */


        //Example with HackIt
        //when instantiating Hackitwrapper need to specify the original class
        /*
        HackItDataQuanta<Integer> hackit2 = new HackItDataQuanta<>(wayang);

        ArrayList<HackitTuple> map = (ArrayList) hackit2.loadCollection(data2, HackitTuple.class)
                .mapHackit(x -> x*2,null,preTag,postTag)
                .filterHackit(x -> x % 3 == 0,null,postTag,preTag)
                .collect(HackitTuple.class);
         */


        //Reduce By Example

        /*
            HackItDataQuanta<Integer> hackit2 = new HackItDataQuanta<>(wayang);
        Collection reduce = hackit2.loadCollection(data,Integer.class)
                .mapNormal(x -> new Tuple2<Integer,Integer>(x%2,x),null,Integer.class, Tuple2.class)
                .reduceByNormal(x -> x.getField0(),(a,b) -> new Tuple2<>(a.getField0(), a.getField1()+b.getField1()), Integer.class, Tuple2.class)
                .collect(Tuple2.class);
        System.out.println(reduce);

         */

/*
        Collection reduce2 = hackit2.loadCollection(data2,HackitTuple.class)
                        .mapHackit(x -> new Tuple2<Integer, Integer>(x % 2,x) ,null)
                .reduceByHackit(x->x.getField0(),(a,b) -> new Tuple2<>(a.getField0(),a.getField1()+b.getField1()),Integer.class,HackitTuple.class,debug,debug)
                                .collect(HackitTuple.class);

        System.out.println(reduce2);


 */





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