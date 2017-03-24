import os
import json
import yelp_utils
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from yelp_utils import *

appName = "Yelper Recommendation System"
conf = SparkConf().setAppName(appName).setMaster("local")
sc = SparkContext(conf=conf)
# sc.setLogLevel("WARN") # config log level to make console less verbose
sqlContext = SQLContext(sc)



#parsed_ratingdata_all_cities = build_rating_df_for_all_cities(sc, sqlContext, userid_to_index_map, bizid_to_index_map)

base_dir = "/Users/hpnhxxwn/Desktop/proj/DE/yelp/review_out3"

# process all major cities
us_cities = [
    ("NC", "Charlotte"),
    ("NV", "Madison"),
    ("WI", "Oregon"),
    ("AZ", "Pheonix"),
    ("PA", "Pittsburgh"),
    ("IL", "Urbana")
]
#canada_cities = [("QC", "canada_montreal")]
#germany_cities = [("BW", "germany_karlsruhe")]
#uk_cities = [("EDH", "uk_edinburgh")]

cities = us_cities
city_names = [p[1] for p in us_cities]

for city_name in city_names:
    builder = RatingDataBuilderForCity(city_name, base_dir, sc)
    #builder.process_one_city(bizid_to_index_map, parsed_ratingdata_all_cities)
    userid_to_index_map = load_userid_to_index_map(city_name)
    bizid_to_index_map = load_bizid_to_index_map(city_name)
    df = build_rating_df_for_one_cities(sc, sqlContext, userid_to_index_map, bizid_to_index_map, city_name)
    file_name = "userid_businessid_star_tuple.csv"
    csv_full_path = os.path.join(base_dir, city_name, file_name)
    print "write tuple to {}".format(csv_full_path)
    df.write.csv(csv_full_path)
    print "Done! tuple written for city {}!".format(city_name)

print "The rating data of all {} cities were successfully processed!".format(len(city_names))