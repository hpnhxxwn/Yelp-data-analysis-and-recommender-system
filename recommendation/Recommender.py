from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from math import sqrt


class Recommender(MRJob):

    def configure_options(self):
        super(Recommender, self).configure_options()
        self.add_file_option('--items', help='Path to business file')

    def load_names(self):
        self.names = {}
        with open('business') as f:
            for line in f:
                fields = line.split('|')
                self.names[int(fields[0])] = fields[1]


    def steps(self):
        return [
            MRStep(mapper = self.mapper_user_ratings,
                   reducer = self.reducer_group_user_ratings),
            MRStep(mapper = self.mapper_create_combinations,
                   reducer = self.reducer_calculate_similarity_score),
            MRStep(mapper = self.mapper_sort_similar_bizs,
                   mapper_init=self.load_names,
                   reducer = self.reducer_group_similar_bizs)
            ]


    def mapper_user_ratings(self, _ , line):
        userId, bizId, rating, timestamp = line.split()
        yield userId, (bizId,float(rating))
    
    def reducer_group_user_ratings(self, userId, biz_ratings):

        ratings = []
        for biz,rating in biz_ratings:
            ratings.append((biz,rating))
        yield userId, ratings

    def mapper_create_combinations(self, userId, ratings_list):
        
       # make a combination of biz rated by the user, 
       #so we can use same combination on other users in reducer

       for rat1, rat2 in combinations(ratings_list, 2):
           
           biz1 = rat1[0]
           rating1 = rat1[1]

           biz2 = rat2[0]
           rating2 = rat2[1]
           
           yield (biz1, biz2), (rating1, rating2)
           yield (biz2, biz1), (rating2, rating1)

    def calculate_cosine_similarity(self,ratingPairs):
        
        ### Cosing similarity 
        # (E a . b / (sqrt(a^2*b^2)

        sum_ab = 0
        sum_aa = 0
        sum_bb = 0
        num_pairs = 0
        score = 0

        for a,b in ratingPairs:
            sum_ab += a*b
            sum_aa += a*a
            sum_bb += b*b
            num_pairs += 1

        numerator = sum_ab
        denominator = sqrt(sum_aa) * sqrt(sum_bb)

        if denominator != 0:
            score = numerator / denominator

        return (score, num_pairs)


    def reducer_calculate_similarity_score(self, bizPair, ratingPairs):

        score, num_pairs = self.calculate_cosine_similarity(ratingPairs)

        if (num_pairs > 3 and score > 0.95):
            yield bizPair, (score, num_pairs)
        
    def mapper_sort_similar_bizs(self, bizPair, score_rating_count):

        try:

            biz1, biz2 = bizPair
            score, total_ratings = score_rating_count

            yield self.names[int(biz1)], self.names[int(biz2)] + '[' + str(score) + ']' + '[' + str(total_ratings) + ']'

        except:
            pass
        

    def reducer_group_similar_bizs(self, biz, similar_bizs_score_count):

        m_bizs = []

        try:
            for biz in similar_bizs_score_count:
                m_bizs.append(biz)

            yield biz, m_bizs

        except:
            pass
        

if __name__ == '__main__':
    Recommender.run()