package org.apache.wayang.java.mapping;

import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.basic.operators.SniffOperator;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.operators.JavaFlatMapOperator;
import org.apache.wayang.java.operators.JavaSniffOperator;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.plugin.hackit.core.sniffer.sniff.Sniff;

import java.util.Collection;
import java.util.Collections;

public class SniffMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        JavaPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "sniff", new SniffOperator<>(null, DataSetType.none(), DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SniffOperator>(
                (matchedOperator, epoch) -> new JavaSniffOperator<>(matchedOperator).at(epoch)
        );
    }
}
