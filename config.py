import time
class Config:
    """
    Internal app configuration not directly used in pipeline
    """
    pickledir = 'pickles'
    edgetonode = 'ETN'
    read_files = 'read_files'
    network='network'
    viz=f'chart_{time.time()}'

if __name__ == '__main__':
    pass