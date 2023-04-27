package org.apache.wayang.spark.mapping;

import org.apache.wayang.basic.operators.SniffOperator;
import org.apache.wayang.basic.operators.TestSniffOperator;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spark.operators.SparkSniffOperator;
import org.apache.wayang.spark.operators.SparkTestSniffOperator;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

public class TestSniffMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "flatMap", new TestSniffOperator<>(null, DataSetType.none(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TestSniffOperator>(
                (matchedOperator, epoch) -> new SparkTestSniffOperator<>(matchedOperator).at(epoch)
        );
    }
}
