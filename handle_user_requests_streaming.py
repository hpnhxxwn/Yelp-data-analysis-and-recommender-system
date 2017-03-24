from __future__ import print_function # this line must appear as the first line!
import os
import sys
import json
import random
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from mf_based_recommender import MFBasedRecommender
from pyspark.mllib.recommendation import MatrixFactorizationModel
#from org.apache.spark import *
from mf_based_recommendation_trainer import MFBasedRecommendationTrainer

def load_model(spark_context):
    base_dir = "/Users/hpnhxxwn/Desktop/proj/DE/yelp/review_out3/"
    city_name = "Charlotte"
    model_file_name = "business_recomm_model_for_{}".format(city_name)
    model_full_path = os.path.join(base_dir, city_name, "mf_based_models", model_file_name)
    model = MatrixFactorizationModel.load(spark_context, model_full_path)
    return model

def recommend_business_for_user(model, user_id, topk=100):
    return model.recommendProducts(user_id, topk)


if __name__ == "__main__":
    sc = SparkContext(appName="UserRequestsStreamingHandler")
    #sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1.5) # 1 second window
    # note that kafka zookeeper default port is 2181 not 9092!
    skQuorum = "localhost:2181"
    topic = ["request"]
    kafkaParams = {"metadata.broker.list":"localhost:9092"}
    #kafkaStream = KafkaUtils.createDirectStream(ssc, topic, kafkaParams)
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'request-group', {'request': 5})

    # init recommender
    #recommender = MFBasedRecommender()
    model = load_model(sc)

    # should broadcast the model. Otherwise, this error will be shown:
    # "It appears that you are attempting to reference SparkContext from a broadcast "
    # Exception: It appears that you are attempting to reference SparkContext from a
    # broadcast variable, action, or transformation. SparkContext can only be used on
    # the driver, not in code that it run on workers. For more information, see SPARK-5063.

    # http://stackoverflow.com/questions/31396323/spark-error-it-appears-that-you-are-attempting-to-reference-sparkcontext-from-a

    # handle kafka streams
    lines = kafkaStream.map(lambda x: x[1])

    def get_user_request(line):
        d = json.loads(line)
        user_id = int(d['user_id'])
        city_name = d['city_name']
        keywords = d['keywords']

        return user_id, city_name, keywords

    def simulate_recommendation(request):
        # simulate the recommendation behavior
        # disable the real recommendation due to performance consideration
        # res = recommend_business_for_user(model, user_id, topk=3)
        # return [r.product for r in res]
        user_id = request[0]
        city_name = request[1]
        keywords = request[2]
        #ids = range(1, 1000)
        #random.shuffle(ids)
        #cut = random.randint(5, 50)
        #sampele_recommended_business_ids = ids[:cut]
        res = recommend_business_for_user(model, user_id, topk=3)
        yelper_logo = \
            " __     __  _                 \n" + \
            " \ \   / / | |                \n" + \
            "  \ \_/ /__| |_ __   ___ _ __ \n" + \
            "   \   / _ \ | '_ \ / _ \ '__|\n" + \
            "    | |  __/ | |_) |  __/ |   \n" + \
            "    |_|\___|_| .__/ \___|_|   \n" + \
            "             | |              \n" + \
            "             |_|              \n"
        prefix = "\n======= Yelper Recommended Results ========\n"
        msg = "user id: {}\ncity: {}\nrequested keywords: {}\n".format(user_id, city_name, str(keywords))
        suffix = "\n"
        for biz in res:
            msg2 = "Recommended business ids: {}\n".format(str(biz))
            print (msg + msg2 + suffix)



        return yelper_logo + prefix + msg + suffix

    # calculate recommended businesses
    recoms = lines.map(get_user_request).map(simulate_recommendation)
    recoms.pprint()

    ssc.start()
    ssc.awaitTermination()