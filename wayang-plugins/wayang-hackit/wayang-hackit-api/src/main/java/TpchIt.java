import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import tpch.GroupKey;
import tpch.LineItemTuple;
import tpch.ReturnTuple;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

public class TpchIt {

    public static void main(String[] args) {
        long t = System.currentTimeMillis();
        WayangContext wayang = new WayangContext().withPlugin(Java.basicPlugin());
        HackItDataQuanta<String> hackit3 = new HackItDataQuanta<>(wayang);
        final int maxShipdate = LineItemTuple.Parser.parseDate("1998-12-01") - 60;
        Collection<HackitTuple> flat = hackit3.loadTextFile(TEST_TXT.toString(),true)
                .map( (line) -> new LineItemTuple.Parser().parse(line),null,String.class, LineItemTuple.class,null,null)
                .filter( (tuple) -> tuple.L_SHIPDATE <= maxShipdate,LineItemTuple.class,null,null)
                .map( (lineItemTuple) -> new ReturnTuple(
                        lineItemTuple.L_RETURNFLAG,
                        lineItemTuple.L_LINESTATUS,
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT),
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT) * (1 + lineItemTuple.L_TAX),
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_DISCOUNT,
                        1),null, LineItemTuple.class,ReturnTuple.class,null,null)
                .reduceBy((returnTuple) -> new GroupKey(returnTuple.L_RETURNFLAG, returnTuple.L_LINESTATUS)
                        , ((t1, t2) -> {
                            t1.SUM_QTY += t2.SUM_QTY;
                            t1.SUM_BASE_PRICE += t2.SUM_BASE_PRICE;
                            t1.SUM_DISC_PRICE += t2.SUM_DISC_PRICE;
                            t1.SUM_CHARGE += t2.SUM_CHARGE;
                            t1.AVG_QTY += t2.AVG_QTY;
                            t1.AVG_PRICE += t2.AVG_PRICE;
                            t1.AVG_DISC += t2.AVG_DISC;
                            t1.COUNT_ORDER += t2.COUNT_ORDER;
                            return t1;
                        })
                        ,GroupKey.class,ReturnTuple.class, null,null)
                .map(v -> {
                    v.AVG_QTY /= v.COUNT_ORDER;
                    v.AVG_PRICE /= v.COUNT_ORDER;
                    v.AVG_DISC /= v.COUNT_ORDER;
                    return v;
                },null,
        ReturnTuple.class,
                ReturnTuple.class,null,null
        )
                //.filter(x->x.getField1() > 10000,Tuple2.class,null,x->{x.addPostTag(debug);return x;})
                .collect(ReturnTuple.class);
        long finish = System.currentTimeMillis();
        System.out.println(flat.size());
        flat.stream().forEach(x-> System.out.println(x));
        System.out.println(finish-t);
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
