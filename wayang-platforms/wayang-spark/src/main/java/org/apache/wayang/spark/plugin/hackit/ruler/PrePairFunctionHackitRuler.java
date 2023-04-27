package org.apache.wayang.spark.plugin.hackit.ruler;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import scala.Tuple2;

import java.util.Set;

public class PrePairFunctionHackitRuler <KeyTuple, KeyPair, InputType, OutputType>
        implements PairFunction<HackitTuple<KeyTuple, InputType>, KeyPair, HackitTuple<KeyTuple, OutputType>> {

    private PairFunction<InputType, KeyPair, OutputType> function;
    private Set<HackitTag> tags;
    public PrePairFunctionHackitRuler(PairFunction<InputType, KeyPair, OutputType> function) {
        this.function = function;
    }

    public PrePairFunctionHackitRuler(PairFunction<InputType, KeyPair, OutputType> function
            ,Set<HackitTag> tags) {
        this.function = function;
        this.tags = tags;
    }

    @Override
    public Tuple2<KeyPair, HackitTuple<KeyTuple, OutputType>> call(HackitTuple<KeyTuple, InputType> v1) throws Exception {
        if(this.tags!=null) v1.addPreTags(this.tags);
        Tuple2<KeyPair, OutputType> result = this.function.call(v1.getValue());
        HackitTuple<KeyTuple, OutputType> hackItTuple_result = new HackitTuple<>(v1.getHeader(), result._2());
        return new Tuple2<>(result._1(), hackItTuple_result);
    }
}
