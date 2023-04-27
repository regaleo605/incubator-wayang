import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import tpch.GroupKey;
import tpch.LineItemTuple;
import tpch.ReturnTuple;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class TpchTest {

    public static void main(String[] args) {
        WayangContext wayang = new WayangContext();
        wayang.register(Spark.basicPlugin());
        TextFileSource lineItemText = new TextFileSource(TEST_TXT.toString(), "UTF-8");
        MapOperator<String, LineItemTuple> parser = new MapOperator<String, LineItemTuple>(
                (line) -> new LineItemTuple.Parser().parse(line), String.class, LineItemTuple.class
        );
        lineItemText.connectTo(0, parser, 0);

        final int maxShipdate = LineItemTuple.Parser.parseDate("1998-12-01") - 60;
        FilterOperator<LineItemTuple> filter = new FilterOperator<>(
                (tuple) -> tuple.L_SHIPDATE <= maxShipdate, LineItemTuple.class
        );
        parser.connectTo(0,filter,0);

        MapOperator<LineItemTuple, ReturnTuple> projection = new MapOperator<>(
                (lineItemTuple) -> new ReturnTuple(
                        lineItemTuple.L_RETURNFLAG,
                        lineItemTuple.L_LINESTATUS,
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT),
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT) * (1 + lineItemTuple.L_TAX),
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_DISCOUNT,
                        1),
                LineItemTuple.class,
                ReturnTuple.class
        );
        filter.connectTo(0,projection,0);

        ReduceByOperator<ReturnTuple, GroupKey> aggregation = new ReduceByOperator<>(
                (returnTuple) -> new GroupKey(returnTuple.L_RETURNFLAG, returnTuple.L_LINESTATUS),
                ((t1, t2) -> {
                    t1.SUM_QTY += t2.SUM_QTY;
                    t1.SUM_BASE_PRICE += t2.SUM_BASE_PRICE;
                    t1.SUM_DISC_PRICE += t2.SUM_DISC_PRICE;
                    t1.SUM_CHARGE += t2.SUM_CHARGE;
                    t1.AVG_QTY += t2.AVG_QTY;
                    t1.AVG_PRICE += t2.AVG_PRICE;
                    t1.AVG_DISC += t2.AVG_DISC;
                    t1.COUNT_ORDER += t2.COUNT_ORDER;
                    return t1;
                }),
                GroupKey.class,
                ReturnTuple.class
        );
        projection.connectTo(0,aggregation,0);

        MapOperator<ReturnTuple, ReturnTuple> aggregationFinalization = new MapOperator<>(
                (t -> {
                    t.AVG_QTY /= t.COUNT_ORDER;
                    t.AVG_PRICE /= t.COUNT_ORDER;
                    t.AVG_DISC /= t.COUNT_ORDER;
                    return t;
                }),
                ReturnTuple.class,
                ReturnTuple.class
        );
        aggregation.connectTo(0, aggregationFinalization, 0);



        List<ReturnTuple> results = new ArrayList<>();
        LocalCallbackSink<ReturnTuple> sink = LocalCallbackSink.createCollectingSink(results,ReturnTuple.class);
        aggregationFinalization.connectTo(0,sink,0);
        wayang.execute(new WayangPlan(sink));
        System.out.println(results);


    }

    public static final URI TEST_TXT = createUri("/lineitem.tbl");

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }
    }
}
