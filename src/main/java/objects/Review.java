package objects; /**
 * Created by hpnhxxwn on 2017/2/21.
 */
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Review {
    @JsonProperty("type")
    public String type;

    @JsonProperty("user")
    public String user;

    @JsonProperty("user_id")
    public String user_id;

    @JsonProperty("review_id")
    public String review_id;

    @JsonProperty("stars")
    public String stars;

    @JsonProperty("text")
    public String text;

    @JsonProperty("business_id")
    public String business_id;

    public class Votes {
        @JsonProperty("funny")
        public String funny;

        @JsonProperty("useful")
        public String useful;

        @JsonProperty("cool")
        public String cool;
    }

    @JsonProperty("votes")
    public Votes votes;

}
