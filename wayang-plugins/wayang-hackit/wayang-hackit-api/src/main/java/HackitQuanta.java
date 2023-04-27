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
import tpch.LineItemTuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HackitQuanta {
    public static void main(String[] args) {
        LineItemTuple.Parser parser = new LineItemTuple.Parser();
        System.out.println("\"3249925\"|\"37271\"|\"9775\"|\"1\"|\"9.00\"|\"10874.43\"|\"0.10\"|" +
                "\"0.04\"|\"N\"|\"O\"|\"1998-04-19\"|\"1998-06-17\"|\"1998-04-21\"|\"TAKE BACK RETURN         \"|" +
                "\"AIR       \"|\"express instructions among the excuses nag\"");
        String line = "3249925|37271|9775|1|9.00|10874.43|0.10|0.04|N|O|1998-04-19|1998-06-17|1998-04-21|DELIVER IN PERSON|TRUCK|pending foxes, slightly red|";
        System.out.println(line.indexOf("|",8));
        System.out.println(line.substring(8,13));
        final LineItemTuple tuple = parser.parse(line);
        System.out.println(tuple.L_SHIPINSTRUCT);
        System.out.println(tuple.L_SHIPMODE);
        System.out.println(tuple.L_COMMENT);

    }


    private static int toDateInteger(int year, int month, int date) {
        final int[] months =new int[]{
                Calendar.JANUARY, Calendar.FEBRUARY, Calendar.MARCH, Calendar.APRIL,
                Calendar.MAY, Calendar.JUNE, Calendar.JULY, Calendar.AUGUST,
                Calendar.SEPTEMBER, Calendar.OCTOBER, Calendar.NOVEMBER, Calendar.DECEMBER
        };
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, months[month - 1]);
        calendar.set(Calendar.DAY_OF_MONTH, date);
        return (int) (calendar.getTimeInMillis() / (1000 * 60 * 60 * 24));
    }
}
