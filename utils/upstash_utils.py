from upstash_vector import Index
from utils.config import Config

class UpstashUtils:
    """
    A utility class for interacting with the Upstash vector database.
    
    Attributes:
        index (Index): An instance of the Index class for interacting with the Upstash vector database.
    """
    def __init__(self):
        cfg = Config()
        url = cfg.get('upstash_url')
        token = cfg.get('upstash_token')
        """
        Initializes the UpstashUtils class with the given URL and token.
        
        Args:
            url (str): The URL of the Upstash vector database.
            token (str): The token for authenticating with the Upstash vector database.
        """
        self.index = Index(url=url, token=token)

    def query(self, vector, top_k=5, include_vectors=False, include_metadata=False, filter=None):
        """
        Queries the Upstash vector database with the given parameters.
        
        Args:
            vector (list): The vector to query.
            top_k (int, optional): The number of top results to return. Defaults to 5.
            include_vectors (bool, optional): Whether to include vectors in the results. Defaults to False.
            include_metadata (bool, optional): Whether to include metadata in the results. Defaults to True.
            filter (str, optional): A filter to apply to the query. Defaults to None.
        
        Returns:
            list: The query results.
        """
        return self.index.query(
            vector=vector,
            top_k=top_k,
            include_vectors=include_vectors,
            include_metadata=include_metadata,
            filter=filter
        )

    def ingest_data(self, vectors):
        """
        Ingests the given vectors into the Upstash vector database.
        
        Args:
            vectors (list): The vectors to ingest. Each vector should be a tuple of the form (id, vector:list, metadata:dict).
                            Example: [
                                ("id1", [0.1, 0.2, 0.3], {"key1": "value1"}),
                                ("id2", [0.4, 0.5, 0.6], {"key2": "value2"})
                            ]
        """
        self.index.upsert(vectors)