from utils.upstash_utils import UpstashUtils
from concurrent.futures import ThreadPoolExecutor
from utils.bigquery_utils import BigQueryClient



class SimpleRecommendationV0:
    def __init__(self):
        self.bq = BigQueryClient()
        self.upstash_db = UpstashUtils()
        ### hyper-parameters
        self.top_k = 10
        self.sample_size = 5


    def fetch_embeddings(self, uri_list):
        """
        Fetches embeddings for the given list of URIs from BigQuery.

        Args:
            uri_list (list): A list of URIs for which to fetch embeddings.

        Returns:
            pandas.DataFrame: A DataFrame containing the embeddings.
        """
        query = f"""
        SELECT uri, ml_generate_embedding_result, metadata
        FROM `yral_ds.video_embeddings`
        WHERE uri IN UNNEST({uri_list})
        """
        return self.bq.query(query)
    
    def fetch_relevant_id(self, vector, watch_history):
        filter_query = "uri not in (" + ",".join([f'"{uri}"' for uri in watch_history]) + ")"
        results = self.upstash_db.query(vector, top_k=self.top_k, filter=filter_query)
        exploit_ids = [result.id for result in results]
        return exploit_ids
        
    def fetch_relevant_ids(self, vector_to_query, watch_history):
        result = []
        with ThreadPoolExecutor(max_workers=len(vector_to_query)) as executor:
            futures = [executor.submit(self.fetch_relevant_id, vector, watch_history=watch_history) for vector in vector_to_query]
            result = []
            for future in futures:
                relevant_ids = future.result()
                result.extend(relevant_ids)
        return result
    
    def get_recommendation(self, successful_plays, watch_history): # entry point function
        vdf = self.fetch_embeddings(successful_plays) # video-dataframe
        vdf = vdf[vdf.ml_generate_embedding_result.apply(lambda x: len(x))==1408]
        sample_vdf = vdf if len(vdf) < self.sample_size else vdf.sample(self.sample_size, random_state=None) # to be replaced with better logic once likes and watch duration is available 
        vector_to_query = sample_vdf.ml_generate_embedding_result.tolist()
        relevant_ids = self.fetch_relevant_ids(vector_to_query, watch_history)
        return relevant_ids


if __name__ == '__main__':
    import time
    start_time = time.time()

    videos_watched = ['gs://yral-videos/cc75ebcdfcd04163bee6fcf37737f8bd.mp4',
    'gs://yral-videos/a8e1035908cc4e6e84f1792f8d655f25.mp4',
    'gs://yral-videos/9072dc569fe24c6d8ed7504d111cd71f.mp4',
    'gs://yral-videos/5054ef5791024da1977b446165aa9fb6.mp4',
    'gs://yral-videos/831fc4a20f974090aa7da3bedc9b0499.mp4',
    'gs://yral-videos/17e4f909dbf14a0b8b13e2b67ea5e54f.mp4',
    'gs://yral-videos/19d5ab6b30914e288db3f8b00dc5ab30.mp4',
    'gs://yral-videos/02b0c85d4da34c9aba1cf48b3b476ce8.mp4',
    'gs://yral-videos/53ec853631a844b48f57e893662daa2c.mp4',
    'gs://yral-videos/b3aac8dad2ef40b6bc987dcda57abd76.mp4']

    rec = SimpleRecommendationV0()
    successful_plays = videos_watched[:6]
    result = rec.get_recommendation(successful_plays=successful_plays, watch_history=videos_watched)
    
    end_time = time.time()
    time_taken = end_time - start_time
    print(f"Time taken: {time_taken:.2f} seconds")
    print(result)

        