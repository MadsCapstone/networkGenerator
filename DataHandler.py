import pandas as pd
from Reader import DBF2DataFrame
from functools import reduce


# TODO Write to file in order to run local
class WriteData:
    pass


# TODO Write to database and store formatted data
class MigrateToDB:
    pass


# TODO add additional methods here in order to not duplicate reads
class DataHandler:
    """
    This class is responsible for acquiring, cleaning, and merging necessary dataframes
    Unfortunately not sure how far we can take the abstraction given we do not know all of
    the project requirements

    It is a class/object factory for necessary pieces to read data from source. Will Eventually expand
    to be used in pipeline and have functions with method calls to orchestrate pipeline pieces.
    """

    def __init__(self, data_dir):
        self.data_dir = data_dir

    def read_dbf_from_file(self, files):
        # read files and based on data_dir
        dbf2df = DBF2DataFrame(self.data_dir, files)
        return dbf2df.get_df()

    def prepare(self):
        pass

    def clean(self):
        pass


if __name__ == '__main__':
    dataHandler = DataHandler('data')
    dataHandler.read_from_file(['NHDFlowline.dbf', 'PlusFlowlineVAA.dbf'])
    # dbfp = DBFFileParser('data')
    # flowline_nodes = dbff.get_dbf_file('PlusFlowlineVAA.dbf')
    # flowline_attrs = dbff.get_dbf_file('NHDFlowline.dbf')
    pass
