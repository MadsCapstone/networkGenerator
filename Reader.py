import shapefile
from os import listdir
from os.path import isfile, join
from collections import defaultdict
from dbfread import DBF
import pandas as pd


class ReadShapeFilesException(Exception):
    pass


class ShapeFileParser:
    """
    Parses all files in a directory and determines if there is a
    shape bunch ('.shp', '.shx', '.dbf')
    These 3 files are required for a "shape" file
    :return
    :parameter directory or filename
    """
    def __init__(self, dir=None, shape_file_name=None):
        """
        :param dir:
        :param shape_file_name:
        Initializes and finds all files in a directory that meet shape file description
        Optionally can get a "bunch" by file name to be read
        """

        #TODO setting up some logic in init just in case
        if dir is None:
            pass
        else:
            pass
        if shape_file_name is None:
            pass
        else:
            pass
        # setup some necessary data
        # attributes self.files has all files in dir
        self.shape_extensions = {'shp', 'shx', 'dbf'}
        self.dir = dir
        self.files = self.__get_files_in_dir(dir)
        bunches = defaultdict(set)
        # loop through the list of files
        for file in self.files:
            fname, ext = self.__get_file_name(file)
            if ext in self.shape_extensions:
                bunches[fname].add(ext)

        #get just the ones that have the files we want
        self.shape_files = self.__filter_on_extension(bunches)

    def __get_files_in_dir(self, dir):
        onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]
        return onlyfiles

    def __get_file_name(self, file):
        name, ext = file.rsplit('.', 1)
        return name, ext

    def __filter_on_extension(self, bunches):
        shape_files = defaultdict()
        for file, exts in bunches.items():
            if exts == self.shape_extensions:
                shape_files[file] = exts
        return shape_files

    def get_shape_files(self):
        return self.shape_files


class ReadShapeFiles:
    def __init__(self, dir):
        self.dir = dir
        sfp = ShapeFileParser(dir)
        shape_files = sfp.get_shape_files()
        self.__data_files = sfp.files
        # array of shape file objects using shapefile.Reader class
        self.shape_files = {file: shapefile.Reader(f'{dir}/{file}') for file in shape_files}

    def get_all_shape_files(self):
        return self.shape_files

    def get_shapefile_bunch_by_name(self, name):
        if name not in self.shape_files:
            raise ReadShapeFilesException(f'Shape File {name} not found in {self.dir} directory')
        else:
            return self.shape_files[name]

    def get_dbf_by_name(self, fname):
        if fname not in self.__data_files:
            raise ReadShapeFilesException(f'File by name: {fname} not found in {self.dir}')
        else:
            return shapefile.Reader(f'{self.dir}/{fname}')


class DBFFileParserException(Exception):
    pass


class DBFFileParser:
    """
    Since we are reading files from a local directory on machine this class
    will look through the directory and make sure before we can request reading a file through our
    pipeline that it is infact present but also a dbf which is a requirement for future steps
    in the pipeline.
    """
    def __init__(self, dir):
        self.dir = dir
        self.files_in_dir = self.__get_files_in_dir(self.dir)
        self.dbf_files_in_dir = self.__filter_files_in_dir(self.files_in_dir)

    def get_dbf_file(self, fname):
        check_dbf = self.__check_if_dbf(fname)
        check_exists = self.__check_if_exists(fname)
        if check_exists:
            return self.__dbf_file_read(fname)
        else:
            return None

    def __dbf_file_read(self, fname):
        dbf = DBF(f'{self.dir}/{fname}', load=True)
        return dbf

    def __check_if_dbf(self, fname):
        name, ext = self.__get_file_name_ext(fname)
        if ext != 'dbf':
            return False
        else:
            return True

    def __check_if_exists(self, fname):
        if fname in self.files_in_dir:
            return True
        else:
            return False

    def __get_file_name_ext(self, file):
        name, ext = file.rsplit('.', 1)
        return name, ext

    def __get_files_in_dir(self, dir):
        onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]
        return onlyfiles

    def __filter_files_in_dir(self, files):
        """
        Filters out files that are not .dbf files
        :param files:
        :return:
        """
        filtered = list(filter(lambda x: self.__check_if_dbf(x), files))
        return filtered


class DBF2DataFrameException(Exception):
    pass


class DBF2DataFrame:
    """
    Provide file to read and read em up
    get a single file from the Parser as a DataFrame
    get a multiple files from the Parser as a dictionary of DataFrames
    """
    # TODO take in a DBFFileParser in init
    def __init__(self, dbfp):
        # dbf file object from dbf reader class
        if dbfp:
            self.__dbfp = dbfp
        else:
            # self.__dbfp = DBFFileParser(data_dir)
            pass

    def get_df(self, files):
        return self.__infer_read_type(files)

    # TODO make return boolean and handle case abstract to class or method
    def __infer_read_type(self, files):
        if type(files) == str:
            return self.__get_dataframe_from_file(files)
        if type(files) == list:
            return self.__get_dataframes_from_files(files)
        else:
            raise DBF2DataFrameException(f'Can only handle args of type str or list not {type(files)}')

    def __dbf_to_dataframe(self, fname):
        records = self.__dbfp.get_dbf_file(fname).records
        return pd.DataFrame(records)

    def __get_dataframe_from_file(self, fname):
        """
        :param dbf:
        :return: dict with dataframe
        """
        return {fname: self.__dbf_to_dataframe(fname)}

    def __get_dataframes_from_files(self, filenames):
        """
        :param dbfs: list of dbf files
        :return: dict of dataframes read from the directory
        """
        return {file: self.__dbf_to_dataframe(file) for file in filenames}

# if __name__ == '__main__':
#     dbfp = DBFFileParser('data')







