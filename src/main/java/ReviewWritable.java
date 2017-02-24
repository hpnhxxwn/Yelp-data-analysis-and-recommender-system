/**
 * Created by hpnhxxwn on 2017/2/21.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//import jdk.nashorn.internal.objects.annotations.Getter;
import org.apache.hadoop.io.Writable;
import lombok.Getter;
import lombok.Setter;

public class ReviewWritable implements Writable {
    @Getter @Setter
    private String text;
    @Getter @Setter
    private String stars;
    @Getter @Setter
    private int posLabel;
    @Getter @Setter
    private int negLabel;

    public void write(DataOutput out) throws IOException {
        out.writeUTF(text);
        out.writeUTF(stars);
        //out.writeUTF(label);
    }

    public void readFields(DataInput in) throws IOException {
        text = in.readUTF();
        stars = in.readUTF();
    }
}
