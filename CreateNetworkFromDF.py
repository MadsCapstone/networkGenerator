import pickle
import pandas as pd
from Reader import DBF2DataFrame
from functools import reduce
import DataHandler as DH
import altair as alt
alt.renderers.enable('altair_viewer')
import networkx as nx
import nx_altair as nxa

# Business Logic Contained Here


# TODO Clean data if necessary
class Cleaner:
    pass


class PrepareDataForVizException(Exception):
    pass


class PrepareDataForViz:
    """
    This class is responsible for gathering data from file and orchestrating
    business logic in order to make the network
    """

    def __init__(self, files):
        self.dh = DH.DataHandler('data')
        self.__raw_data = self.dh.read_dbf_from_file(files)
        # self.__dtype = None
        # self.__set_dtype()
        # TODO Finish logic and make this better for christ sake
        # if self.__dtype == 'dict':
        #     self.prepare(self.__raw_data)
        # if self.__dtype == 'df':
        #     self.prepare_single(self.__raw_data)

    def get_raw_data(self):
        return self.__raw_data

    def prepare(self, dfs=None):
        try:
            # stuff we want to do for each dataframe
            for file, df in self.__raw_data.items():
                # upper case all columns
                self.__raw_data[file] = self.__case_columns(df, True)

            # business logic for combining two dataframes
            dfs = self.__raw_data.values()
            merged_df = self.__merge_data_frames(dfs, col_name='COMID')
            merged_df = self.__remove_columns(merged_df)
            return merged_df
        # Create exception handling
        except Exception:
            print(Exception)

    def __remove_columns(self, df):
        df = df[['COMID', 'GNIS_NAME', 'GNIS_ID', 'FROMNODE', 'TONODE', 'FTYPE']]
        return df

    def __merge_data_frames(self, dfs, col_name=None):
        df = reduce(lambda left, right: pd.merge(left, right, on=col_name), dfs)
        return df

    def __case_columns(self, df, upper=True):
        if upper:
            df.columns = [c.upper() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]
        return df

    # def __set_dtype(self):
    #     if type(self.__raw_data):
    #         self.__dtype = 'dict'
    #     if isinstance(self.__raw_data, pd.DataFrame):
    #         self.__dtype = 'df'
    #     else:
    #         raise PrepareDataForVizException(f'Unknown data type for raw_data needs to be dict or instance of pd.DataFrame not {type(self.__raw_data)}')

class Network:
    def __init__(self):
        pdv = PrepareDataForViz(['NHDFlowline.dbf', 'PlusFlowlineVAA.dbf'])
        self.prepared_data = pdv.prepare()
        self.__G = None
        self.__layout = None

    def __make_Graph(self, graph_type):
        edges = list(self.prepared_data[['FROMNODE', 'TONODE']].to_records(index=False))[:10000]
        self.__G = graph_type()
        self.__G.add_edges_from(edges)

    def __make_layout(self, layout_type):
        self.__layout = layout_type(self.__G)

    def init(self, layout_type=nx.kamada_kawai_layout, graph_type=nx.Graph):
        print('making graph...')
        self.__make_Graph(graph_type)
        print('making layout...')
        self.__make_layout(layout_type)
        print('finished with graph and layout build')

    def get_Graph_Layout(self):
        """
        :return: tuple (Graph, Layout)
        """
        return self.__G, self.__layout

    def save(self):
        with open('network', 'wb') as f:
            pickle.dump(self, f)


class NetworkViz:
    def __init__(self, **kwargs):
        network = Network()
        network.init(**kwargs)
        # going to work this out
        network.save()
        self.G, self.pos = network.get_Graph_Layout()
        self.__viz = None

    def __make_Viz(self, **kwargs):
        self.__viz = nxa.draw_networkx(
            self.G,
            pos=self.pos,
            **kwargs
        )

    def init(self):
        self.__make_Viz()

    def display(self):
        self.__viz.show()




if __name__ == '__main__':
    viz = NetworkViz()
    viz.init()
    viz.display()

