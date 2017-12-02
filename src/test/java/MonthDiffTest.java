import com.customer.udf.MonthDiff;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by yxl on 2017/12/2.
 */
public class MonthDiffTest {

    public static void main(String[] args) throws Exception {
        MonthDiff monthDiff = new MonthDiff();
        Long month  = monthDiff.evaluate("2017-10","2017-12") ;
        System.out.println(month) ;
    }
}
