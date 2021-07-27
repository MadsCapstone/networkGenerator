from Reader import DBF2DataFrame
from functools import reduce
from Singleton import Singleton
from PrepareData import PrepareData
from Reader import DBFFileParser
import pickle
from config import Config
from types import SimpleNamespace


# TODO Write to file in order to run local
class WriteData:
    pass


# TODO Write to database and store formatted data
class MigrateToDB:
    pass


class ReadFileStore:
    def __init__(self, **kwargs):
        self.__read_files_fname = f'{Config.pickledir}/{Config.read_files}'
        try:
            with open(self.__read_files_fname, 'rb') as f:
                self.__dict__.update(pickle.load(f))
        except FileNotFoundError:
            pass

        if kwargs:
            self.__dict__.update(kwargs)
            self.save()
        else:
            pass

    def save(self):
        with open(self.__read_files_fname, 'wb') as f:
            pickle.dump(self.__dict__, f)

    def update(self, d):
        self.__dict__.update(d)
        self.save()


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
        self.__dbfp = DBFFileParser(data_dir)
        self.__dbf2df = DBF2DataFrame(self.__dbfp)
        # self.__pd = PrepareData(self.__dbf2df)
        self.rfs = ReadFileStore()

    # TODO make this less shit because I slapped this together for testing
    def read_df_from_dbf_file(self, files):
        if type(files) == str:
            files = [files]
        dfs = {}
        for file in files:
            d = None
            try:
                d = getattr(self.rfs, file)
            except AttributeError:
                d = self.__dbf2df.get_df(file)
                self.rfs.update(d)
            if d is not None:
                if type(d) is dict:
                    dfs[file] = d[file]
                else:
                    dfs[file] = d
        return dfs

    # TODO: Something is wonky with calling this and external references
    def prepare(self, data):
        pd = PrepareData(self.__dbf2df)
        return pd.prepare(data)

    def create_edge_to_node_network(self):
        pass

    def clean(self):
        pass


if __name__ == '__main__':
    dataHandler = DataHandler('data')
    # dataHandler.read_df_from_dbf_file(['NHDFlowline.dbf', 'PlusFlowlineVAA.dbf'])
    # dataHandler.prepare(['NHDFlowline.dbf', 'PlusFlowlineVAA.dbf'])
    # dbfp = DBFFileParser('data')
    # flowline_nodes = dbff.get_dbf_file('PlusFlowlineVAA.dbf')
    # flowline_attrs = dbff.get_dbf_file('NHDFlowline.dbf')
    pass
