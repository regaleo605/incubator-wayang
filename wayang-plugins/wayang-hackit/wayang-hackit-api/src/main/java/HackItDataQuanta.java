import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.*;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HackItDataQuanta<Out> {

    private ElementaryOperator source;

    private ElementaryOperator operator;

    private int outputIndex = 0;

    private static WayangContext context;

    public HackItDataQuanta(WayangContext context){
        this.context = context;
    }

    public HackItDataQuanta(ElementaryOperator operator, int outputIndex){
        this.operator = operator;
        this.outputIndex = outputIndex;
    }

    //need version with HackitTuple
    public HackItDataQuanta<Out> loadCollection(Collection input, Class<?> cls){
        CollectionSource source = new CollectionSource(input,cls);
        return new HackItDataQuanta<>(source,0);
    }

    public HackItDataQuanta<Tuple2> loadCollectionTuple(Collection input, Class cls){
        CollectionSource source = new CollectionSource(input,cls);
        return new HackItDataQuanta<>(source,0);
    }

    public <Out> HackItDataQuanta<HackitTuple<Header,Out>> loadHackItCollection(Collection<Out> input){
        List<HackitTuple> data = input.stream().map(x -> new HackitTuple(x)).collect(Collectors.toList());
        CollectionSource<HackitTuple> source = new CollectionSource(data,HackitTuple.class);
        return new HackItDataQuanta<>(source,0);
    }

    public HackItDataQuanta<Out> loadTextFile(String path){
        TextFileSource text = new TextFileSource(path);
        return new HackItDataQuanta<>(text,0);
    }

    public HackItDataQuanta<Out> loadTextFile(String path, boolean flag){
        TextFileSource text = new TextFileSource(path,flag);
        return new HackItDataQuanta<>(text,0);
    }

    public <NewOut> HackItDataQuanta<NewOut> mapNormal(FunctionDescriptor.SerializableFunction<Out,NewOut> func, LoadProfileEstimator load,
                                                       Class input, Class output){
        MapOperator<Out,NewOut> map = new MapOperator<>(new TransformationDescriptor<>(func, DataUnitType.createBasic(input),DataUnitType.createBasicUnchecked(output)));
        this.connectTo(map,0);
        return new HackItDataQuanta<>(map,0);
    }

    public <NewOut> HackItDataQuanta<NewOut> map(FunctionDescriptor.SerializableFunction<Out,NewOut> func
            , LoadProfileEstimator load){
        MapOperator map = new MapOperator(func, HackitTuple.class, HackitTuple.class,null,null);
        this.connectTo(map,0);
        return new HackItDataQuanta<>(map,0);
    }

    public <NewOut> HackItDataQuanta<NewOut> map(FunctionDescriptor.SerializableFunction<Out,NewOut> func
            , LoadProfileEstimator load
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> pre
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,NewOut>,HackitTuple<Object,NewOut>> post){
        MapOperator map = new MapOperator(func, HackitTuple.class, HackitTuple.class,pre,post);
        this.connectTo(map,0);
        return new HackItDataQuanta<>(map,0);
    }


    public HackItDataQuanta<Out> filterNormal(FunctionDescriptor.SerializablePredicate<Out> predicate,Class input){
        FilterOperator filter = new FilterOperator(new PredicateDescriptor(predicate,input));
        this.connectTo(filter,0);
        return new HackItDataQuanta<>(filter,0);
    }

    public HackItDataQuanta<Out> filter(FunctionDescriptor.SerializablePredicate<Out> predicate){
        FilterOperator filter = new FilterOperator(new PredicateDescriptor(predicate,HackitTuple.class)
                ,null,null);
        this.connectTo(filter,0);
        return new HackItDataQuanta<>(filter,0);
    }

    public HackItDataQuanta<Out> filter(FunctionDescriptor.SerializablePredicate<Out> predicate
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> pre
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> post){
        FilterOperator filter = new FilterOperator(new PredicateDescriptor(predicate,HackitTuple.class)
                ,new TransformationDescriptor(pre,HackitTuple.class,HackitTuple.class)
                ,new TransformationDescriptor(post,HackitTuple.class,HackitTuple.class));
        this.connectTo(filter,0);
        return new HackItDataQuanta<>(filter,0);
    }

    public HackItDataQuanta<Out> filter(FunctionDescriptor.SerializablePredicate<Out> predicate, Class input
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> pre
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> post){
        FilterOperator filter = new FilterOperator(new PredicateDescriptor(predicate,input)
                ,new TransformationDescriptor(pre,HackitTuple.class,HackitTuple.class)
                ,new TransformationDescriptor(post,HackitTuple.class,HackitTuple.class));
        this.connectTo(filter,0);
        return new HackItDataQuanta<>(filter,0);
    }

    public HackItDataQuanta<Out> reduceHackit(FunctionDescriptor.SerializableBinaryOperator<Out> binOp,  HackitTag preTag,HackitTag postTag){
        GlobalReduceOperator red = new GlobalReduceOperator(new ReduceDescriptor(binOp,HackitTuple.class),preTag,postTag);
        this.connectTo(red,0);
        return new HackItDataQuanta<>(red,0);
    }

    public <K> HackItDataQuanta<Out> reduceBy(FunctionDescriptor.SerializableFunction<Out,K> func,
                                              FunctionDescriptor.SerializableBinaryOperator<Out> bin,
                                              Class key,Class out){
        ReduceByOperator reduce = new ReduceByOperator(func,bin,key,out);
        this.connectTo(reduce,0);
        return new HackItDataQuanta<>(reduce,0);
    }

    public <K> HackItDataQuanta<Out> reduceByNormal(FunctionDescriptor.SerializableFunction<Out,K> func,
                                              FunctionDescriptor.SerializableBinaryOperator<Out> bin,
                                              Class key,Class out){
        ReduceByOperator reduce = new ReduceByOperator(new TransformationDescriptor(func,out,key),
                new ReduceDescriptor(bin,DataUnitType.createGroupedUnchecked(out),DataUnitType.createBasicUnchecked(out))
        ,DataSetType.createDefaultUnchecked(out));
        this.connectTo(reduce,0);
        return new HackItDataQuanta<>(reduce,0);
    }


    public <K> HackItDataQuanta<Out> reduceByHackit(FunctionDescriptor.SerializableFunction<Out,K> func,
                                                    FunctionDescriptor.SerializableBinaryOperator<Out> bin,
                                                    Class key,Class out,Set<HackitTag> preTag, Set<HackitTag> postTag){
        ReduceByOperator reduce = new ReduceByOperator(new TransformationDescriptor(func,out,key),
                new ReduceDescriptor(bin,DataUnitType.createGroupedUnchecked(out),DataUnitType.createBasicUnchecked(out))
                ,DataSetType.createDefaultUnchecked(out),null,null);
        this.connectTo(reduce,0);
        return new HackItDataQuanta<>(reduce,0);
    }

    public <K> HackItDataQuanta<Out> reduceBy(FunctionDescriptor.SerializableFunction<Out,K> func,
                                                    FunctionDescriptor.SerializableBinaryOperator<Out> bin,
                                                    Class key,Class out,
                                              FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> pre
             , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> post){
        ReduceByOperator reduce = new ReduceByOperator(new TransformationDescriptor(func,out,key),
                new ReduceDescriptor(bin,DataUnitType.createGroupedUnchecked(out),DataUnitType.createBasicUnchecked(out))
                ,DataSetType.createDefaultUnchecked(out),new TransformationDescriptor(pre, HackitTuple.class, HackitTuple.class),
                new TransformationDescriptor(post,HackitTuple.class,HackitTuple.class));
        this.connectTo(reduce,0);
        return new HackItDataQuanta<>(reduce,0);
    }

    public <NewOut> HackItDataQuanta<NewOut> flatMapNormal(FunctionDescriptor.SerializableFunction<Out,Iterable<NewOut>> func, Class input, Class output){
        FlatMapOperator flat = new FlatMapOperator(new FlatMapDescriptor(func,input,output));
        this.connectTo(flat,0);
        return new HackItDataQuanta<>(flat,0);
    }

    public <NewOut> HackItDataQuanta<NewOut> flatMap(FunctionDescriptor.SerializableFunction<Out,Iterable<NewOut>> func){
        TransformationDescriptor tmp = null;
        FlatMapOperator flat = new FlatMapOperator(new FlatMapDescriptor(func, HackitTuple.class, HackitTuple.class),tmp,tmp);
        this.connectTo(flat,0);
        return new HackItDataQuanta<>(flat,0);
    }

    public <NewOut> HackItDataQuanta<NewOut> flatMap(FunctionDescriptor.SerializableFunction<Out,Iterable<NewOut>> func
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> pre
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> post){
        FlatMapOperator flat = new FlatMapOperator(new FlatMapDescriptor(func, HackitTuple.class, HackitTuple.class)
                ,new TransformationDescriptor(pre,HackitTuple.class, HackitTuple.class)
                ,new TransformationDescriptor(post,HackitTuple.class,HackitTuple.class));
        this.connectTo(flat,0);
        return new HackItDataQuanta<>(flat,0);
    }

    //For TextFile
    public <NewOut> HackItDataQuanta<NewOut> flatMap(FunctionDescriptor.SerializableFunction<Out,Iterable<NewOut>> func
            ,Class input,Class output){
        TransformationDescriptor tmp = null;
        FlatMapOperator flat = new FlatMapOperator(new FlatMapDescriptor(func, input, output),tmp,tmp);
        this.connectTo(flat,0);
        return new HackItDataQuanta<>(flat,0);
    }

    public <NewOut> HackItDataQuanta<NewOut> flatMap(FunctionDescriptor.SerializableFunction<Out,Iterable<NewOut>> func
            ,Class input,Class output
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> pre
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> post){
        FlatMapOperator flat = new FlatMapOperator(new FlatMapDescriptor(func, input, output)
                ,new TransformationDescriptor(pre,HackitTuple.class, HackitTuple.class)
                ,new TransformationDescriptor(post,HackitTuple.class,HackitTuple.class));
        this.connectTo(flat,0);
        return new HackItDataQuanta<>(flat,0);
    }

    public <NewOut> HackItDataQuanta<NewOut> map(FunctionDescriptor.SerializableFunction<Out,NewOut> func
            , LoadProfileEstimator load, Class input, Class output
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> pre
            , FunctionDescriptor.SerializableFunction<HackitTuple<Object,NewOut>,HackitTuple<Object,NewOut>> post){
        MapOperator map = new MapOperator(func, input, output,pre,post);
        this.connectTo(map,0);
        return new HackItDataQuanta<>(map,0);
    }



    public HackItDataQuanta<Out> sniff(){
        FunctionDescriptor.SerializableFunction<Out,Iterable<Out>> func = null;
        SniffOperator sniff = new SniffOperator(new FlatMapDescriptor(null, HackitTuple.class, HackitTuple.class));
        this.connectTo(sniff,0);
        return new HackItDataQuanta<>(sniff,0);
    }

    public HackItDataQuanta<Out> sniffText(){
        FunctionDescriptor.SerializableFunction<Out,Iterable<Out>> func = null;
        SniffOperator sniff = new SniffOperator(new FlatMapDescriptor(null, String.class, String.class));
        this.connectTo(sniff,0);
        return new HackItDataQuanta<>(sniff,0);
    }

    public HackItDataQuanta<Out> sniff(Class type){
        FunctionDescriptor.SerializableFunction<Out,Iterable<Out>> func = null;
        SniffOperator sniff = new SniffOperator(new FlatMapDescriptor(null, type, type));
        this.connectTo(sniff,0);
        return new HackItDataQuanta<>(sniff,0);
    }

    public HackItDataQuanta<Out> tsniff(Class type){
        FunctionDescriptor.SerializableFunction<Out,Iterable<Out>> func = null;
        TestSniffOperator tsniff = new TestSniffOperator(new FlatMapDescriptor(null, type, type));
        this.connectTo(tsniff,0);
        return new HackItDataQuanta<>(tsniff,0);
    }


    public <ThatOut,Key> HackItDataQuanta<Tuple2<Out,ThatOut>> join(HackItDataQuanta<ThatOut> that,
                                                                FunctionDescriptor.SerializableFunction<Out,Key> keyExtractor0,
                                                                FunctionDescriptor.SerializableFunction<Out,Key> keyExtractor1,
                                                                Class input0, Class input1, Class key){

        JoinOperator join = new JoinOperator(
                        new TransformationDescriptor(keyExtractor0,DataUnitType.createBasic(input0),DataUnitType.createBasicUnchecked(key)),
                        new TransformationDescriptor(keyExtractor1,DataUnitType.createBasic(input1),DataUnitType.createBasicUnchecked(key))
        );

        this.connectTo(join,0);
        that.connectTo(join,1);
        return new HackItDataQuanta<>(join,0);
    }

    public <ThatOut,Key> HackItDataQuanta<Tuple2<Out,ThatOut>> joinHackit(HackItDataQuanta<ThatOut> that,
                                                                    FunctionDescriptor.SerializableFunction<Out,Key> keyExtractor0,
                                                                    FunctionDescriptor.SerializableFunction<Out,Key> keyExtractor1,
                                                                    Class key,HackitTag preTag, HackitTag postTag){

        JoinOperator join = new JoinOperator(
                new TransformationDescriptor(keyExtractor0,DataUnitType.createBasic(HackitTuple.class),DataUnitType.createBasicUnchecked(key)),
                new TransformationDescriptor(keyExtractor1,DataUnitType.createBasic(HackitTuple.class),DataUnitType.createBasicUnchecked(key)),
                preTag,postTag
        );

        this.connectTo(join,0);
        that.connectTo(join,1);
        return new HackItDataQuanta<>(join,0);
    }

    public <ThatOut,Key> HackItDataQuanta<Tuple2<Out,ThatOut>> joinHackit(HackItDataQuanta<ThatOut> that,
                                                                          FunctionDescriptor.SerializableFunction<Out,Key> keyExtractor0,
                                                                          FunctionDescriptor.SerializableFunction<Out,Key> keyExtractor1,
                                                                          Class key,Set<HackitTag> preTag, Set<HackitTag> postTag){

        JoinOperator join = new JoinOperator(
                new TransformationDescriptor(keyExtractor0,DataUnitType.createBasic(HackitTuple.class),DataUnitType.createBasicUnchecked(key)),
                new TransformationDescriptor(keyExtractor1,DataUnitType.createBasic(HackitTuple.class),DataUnitType.createBasicUnchecked(key)),
                preTag,postTag
        );

        this.connectTo(join,0);
        that.connectTo(join,1);
        return new HackItDataQuanta<>(join,0);
    }



    public HackItDataQuanta<Out> repeat(int n, Class type, Function<HackItDataQuanta<Out>,HackItDataQuanta<Out>> body) {
        RepeatOperator<Out> repeat = new RepeatOperator(n,type);
        this.connectTo(repeat,RepeatOperator.INITIAL_INPUT_INDEX);

        HackItDataQuanta<Out> loop = new HackItDataQuanta<>(repeat, RepeatOperator.ITERATION_OUTPUT_INDEX);
        HackItDataQuanta<Out> iter_result = body.apply(loop);
        iter_result.connectTo(repeat,RepeatOperator.ITERATION_INPUT_INDEX);

        return new HackItDataQuanta<>(repeat,RepeatOperator.FINAL_OUTPUT_INDEX);
    }


    public void connectTo(ElementaryOperator operator, int inputIndex){
        this.operator.connectTo(this.outputIndex,operator,inputIndex);
    }

    public void writeToText(String url,FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,String> change,Class cls){
        List results = new ArrayList();
        TextFileSink sink = new TextFileSink(url,change,cls);
        this.connectTo(sink,0);

        context.execute(new WayangPlan(sink));
    }

    public Collection collect(Class cls){
        List results = new ArrayList();
        LocalCallbackSink sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(cls));
        this.connectTo(sink,0);

        context.execute(new WayangPlan(sink));

        return results;
    }

    public Collection collectHackit(Class cls){
        List results = new ArrayList();
        LocalCallbackSink sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(cls),true);
        this.connectTo(sink,0);

        context.execute(new WayangPlan(sink));

        return results;
    }

    public HackItDataQuanta<Out> withName(String name){
        this.operator.setName(name);
        return this;
    }

    public HackItDataQuanta<Out> withCardinalityEstimator(CardinalityEstimator estimator){
        this.operator.setCardinalityEstimator(this.outputIndex,estimator);
        return this;
    }

    public <Something> HackItDataQuanta<Out> withBroadcast(HackItDataQuanta<Something> sender, String broadcastName){
        sender.broadcast(this,broadcastName);
        return this;
    }

    private <Something> void broadcast(HackItDataQuanta<Something> receiver,String broadcastName){
        receiver.registerBroadCast(this.operator,this.outputIndex,broadcastName);
    }

    private void registerBroadCast(ElementaryOperator sender,int outputIndex,String broadcastName){
        sender.broadcastTo(outputIndex,this.operator,broadcastName);
    }

    public HackItDataQuanta<Out> loadTextFile(String path, FunctionDescriptor.SerializableFunction<HackitTuple<Object,Out>,HackitTuple<Object,Out>> func){
        TextFileSource text = new TextFileSource(path,"UTF-8",new TransformationDescriptor(func,DataUnitType.createBasic(HackitTuple.class),DataUnitType.createBasicUnchecked(HackitTuple.class)));
        return new HackItDataQuanta<>(text,0);
    }

}
