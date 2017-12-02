import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by yxl on 2017/12/2.
 */
public class MonthDiffTest {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM") ;
        Calendar startCalendar = Calendar.getInstance() ;
        startCalendar.setTime(sdf.parse("2015-03")) ;
        System.out.println(startCalendar.getTime() + "\t" + startCalendar.getTimeInMillis());
        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(sdf.parse("2015-03"));
        System.out.println(endCalendar.getTime() + "\t" + endCalendar.getTimeInMillis());
        long month =(endCalendar.getTimeInMillis() - startCalendar.getTimeInMillis()) / (3600 * 1000 * 24 * 30l);
        System.out.println(month);
    }
}
