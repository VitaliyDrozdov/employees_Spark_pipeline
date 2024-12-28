import os
from dotenv import load_dotenv


class Config:
    def __init__(self, *args, **kwargs):
        load_dotenv()

        self.BASE_URL = os.getenv("BASE_URL")
        self.LOGIN_URL = os.getenv("LOGIN_URL")
        self.NF_USERNAME = os.getenv("NF_USERNAME")
        self.NF_PASSWORD = os.getenv("NF_PASSWORD")
        self.DRIVER_PATH = os.getenv("DRIVER_PATH")
        self.HDFS_PATH = os.getenv("HDFS_PATH")
        self.FILE_MASK = os.getenv("FILE_MASK")
        self.OUTPUT_FILE = os.getenv("OUTPUT_FILE")
        self.LOCAL_DIR = os.getenv("LOCAL_DIR")

        for k, v in kwargs.items():
            setattr(self, k, v)
