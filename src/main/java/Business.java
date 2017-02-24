/**
 * Created by hpnhxxwn on 2017/2/21.
 */
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import java.util.List;

public class Business {
    @JsonProperty("user_id")
    public String user_id;

    @JsonProperty("business_id")
    public String business_id;

    @JsonProperty("full_address")
    public String full_address;

    @JsonProperty("open")
    public boolean open;

    @JsonProperty("categories")
    public List<String> categories;

    @JsonProperty("stars")
    public String stars;

    @JsonProperty("review_count")
    public String reviewCount;
}
