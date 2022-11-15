import org.apache.wayang.api.dataquanta.DataQuanta;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.plugin.hackit.core.tags.PauseTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HackitQuanta {
    public static void main(String[] args) {
        HackitTuple tuple = new HackitTuple(3);
        System.out.println(tuple.getHeader().isHaltJob());
        tuple.addPreTag(new PauseTag());
        System.out.println(tuple.getHeader().isHaltJob());
        tuple.getHeader().clearTags();
        System.out.println(tuple.getHeader().isHaltJob());
        tuple.addPreTag(new PauseTag());
        System.out.println(tuple.getHeader().isHaltJob());
    }
}
