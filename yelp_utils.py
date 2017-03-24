import os
import json
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# ---------------------------------------------------------------------------


class RatingDataBuilderForCity:
    def __init__(self, city_name, base_dir, spark_context):
        self.city_name = city_name
        self.base_dir = base_dir
        self.spark_context = spark_context

    def __get_indexed_business_ids_for_city(self, bizid_to_index_map):
        file_name = "{}_business.csv".format(self.city_name)
        full_path = os.path.join(self.base_dir, self.city_name, file_name)
        with open(full_path, "r") as f:
            lines = f.readlines()

        lines = lines[1:] # discard header line
        # get all business ids in integer by looking up pre-saved map
        indexed_business_ids = []

        for r in lines:
            sp = r.split(',')
            raw_business_id = sp[1].strip()
            b = bizid_to_index_map[raw_business_id]
            indexed_business_ids.append(b)
        print "indexed_business_ids populated!"
        return indexed_business_ids

    def __get_and_write_userid_businessid_star_tuple_in_city(self, indexed_business_ids, tuple_df_entire_biz):
        print "convert python list to rdd"
        bid_rdd = self.spark_context.parallelize(indexed_business_ids)

        print "add a header row to the rdd such that later I can do dataframe join"
        row = Row("business_id_in_one_city")
        b_df = bid_rdd.map(row).toDF()

        print "inner join the entire business df with the small df (local businesses in one city)"
        joined = tuple_df_entire_biz.join(b_df, tuple_df_entire_biz.business_id==b_df.business_id_in_one_city, 'inner')
        df_tuple_for_one_city = joined.select('user_id', 'business_id', 'star')

        file_name = "userid_businessid_star_tuple.csv"
        csv_full_path = os.path.join(self.base_dir, self.city_name, file_name)

        print "write tuple to {}".format(csv_full_path)
        df_tuple_for_one_city.write.csv(csv_full_path)
        print "Done! tuple written for city {}!".format(self.city_name)

    def process_one_city(self, bizid_to_index_map, tuple_df_entire_biz):
        print "Processing {}".format(self.city_name)
        indexed_business_ids = self.__get_indexed_business_ids_for_city(bizid_to_index_map)
        print "total businesses in {}: {}".format(self.city_name, len(indexed_business_ids))
        self.__get_and_write_userid_businessid_star_tuple_in_city(indexed_business_ids, tuple_df_entire_biz)


# ---------------------------------------------------------------------------
def load_userid_to_index_map(city_name):
    userid_to_index_map = {}
    with open('/Users/hpnhxxwn/Desktop/proj/DE/yelp/review_out3/' + city_name + "_recommender-system-r-00000", 'r') as fp:
        for line in fp:
            l = line.split(" ")
            if l[0] not in userid_to_index_map:
                userid_to_index_map.update({l[0]:len(userid_to_index_map)})

        #userid_to_index_map = json.load(fp)
        print "review_id_int_map json file loaded!"
    return userid_to_index_map

def load_bizid_to_index_map(city_name):
    bizid_to_index_map = {}
    with open('/Users/hpnhxxwn/Desktop/proj/DE/yelp/review_out3/' + city_name + "_recommender-system-r-00000", 'r') as fp:
        for line in fp:
            l = line.split(" ")
            if l[0] not in bizid_to_index_map:
                bizid_to_index_map.update({l[0]:len(bizid_to_index_map)})

    return bizid_to_index_map

# ---------------------------------------------------------------------------
def build_rating_df_for_all_cities(spark_context, sql_context,
                                   userid_to_index_map, bizid_to_index_map):
    # load the saved review csv file
    review_data_dir = "/Users/sundeepblue/Bootcamp/allweek/week9/capstone/data/yelp_data/split_review_data"
    csv_file_name = "full_userId_businessId_star_tuple.csv"
    rating_raw_data = spark_context.textFile(os.path.join(review_data_dir, csv_file_name))

    header = rating_raw_data.take(1)[0]

    ratings_data = rating_raw_data \
        .filter(lambda line: line != header) \
        .map(lambda line: line.split(",")) \
        .map(lambda tokens: (tokens[1],tokens[2],tokens[3])) \
        .toDF(['user_id', 'business_id', 'stars'])

    ratings_data.show()

    assert ratings_data.select('user_id').count() == 2685066
    assert ratings_data.select('user_id').distinct().count() == 686556

    def convert_row(row):
        new_user_id = userid_to_index_map[row.user_id]
        new_business_id = bizid_to_index_map[row.business_id]
        stars = int(row.stars)
        #return Row(user_id=new_user_id, business_id=new_business_id, stars=stars) # no need to return Row
        return (new_user_id, new_business_id, stars)

    converted_ratings_data = ratings_data.rdd.map(convert_row).cache()
    print converted_ratings_data.count()
    print "converted_ratings_data done"

    print "prepare converted_ratings_data dafaframe ..."
    parsed_ratingdata_for_all_cities = sql_context.createDataFrame(converted_ratings_data, ['user_id', "business_id", "star"])
    parsed_ratingdata_for_all_cities.printSchema()

    return parsed_ratingdata_for_all_cities

def build_rating_df_for_one_cities(spark_context, sql_context,
                                   userid_to_index_map, bizid_to_index_map, city_name):
    # load the saved review csv file
    review_data_dir = "/Users/hpnhxxwn/Desktop/proj/DE/yelp/review_out3/"
    csv_file_name = city_name + "_recommender-system-r-00000"
    rating_raw_data = spark_context.textFile(os.path.join(review_data_dir, csv_file_name))

    header = rating_raw_data.take(1)[0]

    ratings_data = rating_raw_data \
        .filter(lambda line: line != header) \
        .map(lambda line: line.split(" ")) \
        .map(lambda tokens: (tokens[0],tokens[1],tokens[2])) \
        .toDF(['user_id', 'business_id', 'stars'])

    ratings_data.show()
    print ("Total number of user reviews is " + str(ratings_data.select('user_id').count()))
    print ("Distinct number of user reviews is " + str(ratings_data.select('user_id').distinct().count()))


    def convert_row(row):
        new_user_id = userid_to_index_map[row.user_id]
        new_business_id = bizid_to_index_map[row.business_id]
        stars = int(row.stars)
        #return Row(user_id=new_user_id, business_id=new_business_id, stars=stars) # no need to return Row
        return (new_user_id, new_business_id, stars)

    converted_ratings_data = ratings_data.rdd.map(convert_row).cache()
    print converted_ratings_data.count()
    print "converted_ratings_data done"

    print "prepare converted_ratings_data dafaframe ..."
    parsed_ratingdata_for_one_city = sql_context.createDataFrame(converted_ratings_data, ['user_id', "business_id", "star"])
    parsed_ratingdata_for_one_city.printSchema()

    return parsed_ratingdata_for_one_city

# ---------------------------------------------------------------------------
def load_and_parse_ratingdata_for_city(city_name, base_dir, spark_session):
    # load data for one city
    file_name = "userid_businessid_star_tuple.csv"
    #csv_full_path = base_dir + "/" + city_name + file_name
    csv_full_path = os.path.join(base_dir, city_name, file_name, city_name + ".csv")

    ratingSchema = StructType([StructField("user_id", IntegerType(), True),
                               StructField("business_id", IntegerType(), True),
                               StructField("stars", IntegerType(), True)])

    df = spark_session.read.csv(path=(csv_full_path + ""), sep=u",", schema=ratingSchema)

    df_rdd = df.rdd
    print df_rdd.take(3)

    def convert_row(row):
        user_id = int(row.user_id)
        business_id = int(row.business_id)
        star = int(row.stars)
        return (user_id, business_id, stars)

    converted_df_rdd = df_rdd.map(lambda x: (long(x.user_id), long(x.business_id), int(x.stars)))
    print converted_df_rdd.take(3)
    return converted_df_rdd