import time
import json
import random
import msgpack
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'),
                         retries=5)


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

sample_keywords = ["restaurants", "ice cream", "dinner", "eat", "lunch", "bar", "ramen", "chinese food"
                                                                                                      "asianfusion", "newamerican", "cocktailbars", "italian", "japanese", "personalchefs", "Mexican", "club", "best indian food",
                   "movie"]

# Asynchronous by default
for i in range(100):
    random_user_id = random.randint(1e6, 1e8)
    _idx = random.randint(0, len(city_names)-1)
    random_city_name = city_names[_idx]
    _idx_keywords = random.randint(0, len(sample_keywords)-1)
    random_sample_keywords = sample_keywords[_idx_keywords]

    payload = {
        'user_id': random_user_id,
        'city_name': random_city_name,
        'keywords': random_sample_keywords
    }
    print "User {} wanted recommendation for {} in city {}".format(random_user_id, random_sample_keywords, random_city_name)
    future = producer.send('request', payload)
    time.sleep(2*random.random())

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)

producer.flush()