from functools import reduce
import pandas as pd
from collections import defaultdict

class PrepareDataException(Exception):
    pass

class PrepareData:
    """
    This class is responsible for gathering data from file and orchestrating
    business logic in order to prepare the data to be analyzed for making the network

    Can and will evolve for changes in the source data

    The only current tasks for this
    * upper case all columns so that they can be used as keys
    * join data in dataframe on key
    * select columns to keep and return for use in DataHandler factory
    """
    def __init__(self,
                 dbf2df=None,
                 merge_instruction=None,
                 cols_to_keep=['COMID', 'GNIS_NAME', 'GNIS_ID', 'FROMNODE', 'TONODE', 'FTYPE'],
                 keep_cols=False
                 ):
        # self.__dbf2df = dbf2df
        self.__keep_cols = keep_cols
        self.__preparedData = None

    def prepare(self, data):
        try:
            # business logic for combining two dataframes
            # add unnamed names
            data['NHDFlowline.dbf'] = self.__add_unnamed_counter(data['NHDFlowline.dbf'])
            # stuff we want to do for each dataframe
            for file, df in data.items():
                # upper case all columns
                data[file] = self.__case_columns(df, True)

            # merge everything now
            dfs = data.values()
            merged_df = self.__merge_data_frames(dfs, col_name='COMID')
            if self.__keep_cols:
                merged_df = self.__keep_columns(merged_df)
            self.__preparedData = merged_df
            return merged_df

        # Create exception handling
        except Exception as e:
            raise PrepareDataException(e)

    def __add_unnamed_counter(self, df):
        counts = defaultdict(int)
        def namer(r):
            if r.GNIS_NAME == '':
                counts[r.FTYPE] += 1
                name = f'Unnamed {r.FTYPE} {counts[r.FTYPE]}'
            else:
                name = r.GNIS_NAME
            return name
        df['GNIS_NAME'] = df.apply(namer, axis=1)
        return df


    def __keep_columns(self, df):
        df = df[self.__keep_cols]
        return df

    def __merge_data_frames(self, dfs, col_name=None):
        df = reduce(lambda left, right: pd.merge(left, right, on=col_name, how='inner'), dfs)
        return df

    def __case_columns(self, df, upper=True):
        if upper:
            df.columns = [c.upper() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]
        return df