package objects;

/**
 * Created by hpnhxxwn on 2017/3/16.
 */
import lombok.Getter;
import lombok.Setter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class YelpBusinessAPI {
    @Getter @Setter
    public String user_id;

    @Getter @Setter
    public String business_id;

    @Getter @Setter
    public List<String> full_address;

    @Getter @Setter
    public boolean open;

    @Getter @Setter
    public List<List<String>> categories;

    @Getter @Setter
    public String stars;

    @Getter @Setter
    public String reviewCount;

    @Getter @Setter
    public String name;
    public YelpBusinessAPI(){}
    public YelpBusinessAPI(String uid, String bid, String addr, String open, List<List<String>> catetories, String rating, String review_count, String name) {

    }
}
