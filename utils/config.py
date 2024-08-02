import os


class Config:
    def __init__(self):
        self.config_data = self.load_config()

    def load_config(self):
        return {
            'upstash_url': os.getenv('UPSTASH_URL'),
            'upstash_token': os.getenv('UPSTASH_TOKEN'),
            'project': os.getenv('PROJECT'),
            'service_cred': os.getenv('SERVICE_CRED')
        }

    def get(self, key, default=None):
        return self.config_data.get(key, default)

if __name__ == '__main__':
    cfg = Config()
    print(cfg.config_data)
