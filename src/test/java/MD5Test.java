import com.customer.udf.MD5;
import com.google.common.hash.Hashing;

/**
 * Created by yxl on 2017/12/2.
 */
public class MD5Test {
    public static void main(String[] args) throws Exception {
        MD5 md5 = new MD5();
        Integer [] arguments = {1000078,1000079,1000080,1000081} ;
        StringBuffer sb = new StringBuffer() ;
        for (int i = 0; i < arguments.length; i++) {
            sb.append(arguments[i].toString()) ;
        }
        System.out.println(Hashing.md5().hashString(sb.toString()).toString());
    }
}
