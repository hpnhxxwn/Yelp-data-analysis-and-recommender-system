package objects; /**
 * Created by hpnhxxwn on 2017/2/24.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//import jdk.nashorn.internal.objects.annotations.Getter;
import org.apache.hadoop.io.Writable;
import lombok.Getter;
import lombok.Setter;
public class BusinessWritable implements Writable {
    @Getter @Setter
    private String businessId;

    //@Getter @Setter
    //private String[] categories;

    public void write(DataOutput out) throws IOException {
        out.writeUTF(businessId);
        //out.writeUTF(categories);
    }

    public void readFields(DataInput in) throws IOException {
        businessId = in.readUTF();
    }
}
