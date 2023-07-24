import os


class Config:

    def __init__(self):
        if "BASE_URI" in os.environ:
            self.BASE_URI = os.environ["BASE_URI"]
        else:
            raise KeyError("BASE_URI not in environment.")
