package org.apache.wayang.spark.plugin.hackit;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.wayang.java.Java;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;
import org.apache.wayang.spark.plugin.hackit.ruler.Function2HackitRuler;
import scala.Tuple2;

import java.util.Set;

public class HackitPairRDD <K, KT, V>{
    private JavaPairRDD<KT, HackitTuple<K, V>> rdd;

    public HackitPairRDD(JavaPairRDD<KT, HackitTuple<K, V>> rdd) {
        this.rdd = rdd;
    }

    public JavaPairRDD<KT, HackitTuple<K,V>> getPairRDD(){return this.rdd;}
/*
    public HackitPairRDD<K, KT, V> reduceByKey(Function2<V, V, V> func) {
        return new HackitPairRDD<>(
                this.rdd
                        .mapValues(
                                HackitTuple::getValue
                        )
                        .reduceByKey(
                                func
                        )
                        .mapValues(
                                HackitTuple::new
                        )
        );
    }


 */
    public HackitPairRDD<K, KT, V> reduceByKey(Function2<V, V, V> func) {
        return new HackitPairRDD<>(
                this.rdd.reduceByKey(new Function2HackitRuler<>(func))
        );
    }

    public HackitPairRDD<K, KT, V> reduceByKey(Function2<V, V, V> func, Set<HackitTag> preTag, Set<HackitTag> postTag) {
        return new HackitPairRDD<>(
                this.rdd.reduceByKey(new Function2HackitRuler<>(func,preTag,postTag))
        );
    }

    public <O> JavaPairRDD<KT, HackitTuple<Object, Tuple2<V,O>>> join(HackitPairRDD<K, KT, O> other){
        JavaPairRDD<KT,V> one = this.rdd.mapValues(x ->x.getValue());
        JavaPairRDD<KT,O> otherOne = other.rdd.mapValues(x ->x.getValue());
        JavaPairRDD<KT,Tuple2<V,O>> result = one.join(otherOne);
        JavaPairRDD<KT,HackitTuple<Object,Tuple2<V,O>>> re = result.mapValues(x -> new HackitTuple<>(x));
        //JavaPairRDD<KT, Tuple2<HackitTuple<K, V>, HackitTuple<K, V>>> tmp = this.rdd.join(other.rdd);
        return re;
    }

    public <O> JavaPairRDD<KT, HackitTuple<Object, Tuple2<V,O>>> join(HackitPairRDD<K, KT, O> other,Set<HackitTag> preTag,Set<HackitTag> postTag){
        JavaPairRDD<KT,Tuple2<HackitTuple<K,V>,HackitTuple<K,O>>> join = this.rdd.join(other.rdd);
        JavaPairRDD<KT,HackitTuple<Object,Tuple2<V,O>>> something = join.mapValues(x->{
            Header merge = x._1().getHeader().mergeHeaderTags(x._2);
            merge.addPreTags(preTag);
            merge.addPostTags(postTag);
            HackitTuple tuple = new HackitTuple(merge,new Tuple2<>(x._1.getValue(),x._2.getValue()));
            return tuple;
        });
        return something;
    }



/*
    /**
     * Wrap the RDD inside of DebugRDD
     * *
    public static <T> DebugPairRDD<T> fromRDD(RDD<T> rdd){
        return this(rdd, rdd.elementClassTag());
    }

    /**
     * Wrap the JavaRDD inside of DebugRDD
     * *
    public static <T> DebugPairRDD<T> fromJavaRDD(JavaRDD<T> rdd){
        return new DebugRDD<T>(rdd.rdd(), rdd.classTag());
    }

    /**
     * unwrap the RDD and coming back to the normal execution
     * *
    public RDD<Tuple2<K, V>> toRDD(){
        return this.toPairJavaRDD().rdd();
    }

    /**
     * unwrap the RDD and coming back to the normal execution
     * *
    public JavaPairRDD<K, V> toPairJavaRDD(){
        RDD<Tuple2<K, V>> tmp = this.rdd();
        if(this.classTag().getClass() == HackItTuple.class) {
            tmp = null;//((JavaRDD<HackItTuple<T>>)this.rdd).map(com.qcri.hackit -> com.qcri.hackit.getValue());
        }
        return super.fromRDD(tmp, this.kClassTag(), this.vClassTag());
    }

    public static <T> DebugRDD<T> wrapDebugRDD(JavaRDD<T> rdd){
        return DebugRDD.fromJavaRDD(rdd);
    }


    @Override
    public Map<K, V> reduceByKeyLocally(Function2<V, V, V> func) {
        return super.reduceByKeyLocally(func);
    }

    @Override
    public <W> JavaPairRDD<K, Tuple2<V, W>> join(JavaPairRDD<K, W> other) {
        return super.join(other);
    }

    @Override
    public void saveAsTextFile(String path) {
        super.saveAsTextFile(path);
    }*/
}
