import com.customer.udf.WeekEnd;
import com.customer.udf.WeekFirst;

import java.util.Date;

/**
 * Created by yxl on 2017/12/29.
 */
public class WeekFirstTest {
    public static void main(String[] args){
        WeekEnd wf = new WeekEnd() ;
        Date date = new Date();
        String str = wf.evaluate("2017-12-20") ;
        System.out.println(str) ;
    }
}
