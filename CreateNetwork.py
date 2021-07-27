from DataHandler import DataHandler
import pprint
import copy
import pandas as pd
from PrepareData import PrepareData
from tqdm import tqdm
import networkx as nx
import nx_altair as nxa
import pickle
from config import Config

pd.set_option("display.max_rows", None, "display.max_columns", None)


def printt(df):
    print(df.to_markdown(floatfmt=''))


class NodeLayout:
    edge = None
    midnodes = None
    allnodes = None


class BranchDetails:
    def __init__(self, branch_df, alldf):
        self.branch_df = branch_df
        self.edges = list
        self.branch_map = dict()
        self.mainbranch = None
        self.node_layout = dict()
        self.__make_branch_attributes(alldf)

    def __make_branch_attributes(self, alldf):
        df = self.branch_df
        alldf.StreamLeve.unique()
        for row in df.itertuples():
            segment_streamlevel = row.StreamLeve
            segment_levelpathid = row.LevelPathI
            segment_comid = row.ComID
            next_order_branches = alldf[
                (alldf.StreamLeve == segment_streamlevel + 1)
                & (alldf.DnLevelPat == segment_levelpathid)
                ]
            if len(next_order_branches) > 0:
                self.branch_map[segment_comid] = next_order_branches
            else:
                self.branch_map[segment_comid] = None
            # self.node_layout[segment_comid] = self.get_mainstem_node_layout(row)

    # TODO: Is this necessary at all now? Creating node layouts
    def get_mainstem_node_layout(self, mainstem_rows):
        df = mainstem_rows[['FromNode', 'ToNode']]
        from_to = dict(df.values)
        to_from = dict(mainstem_rows[['ToNode', 'FromNode']])
        from_node = None
        to_node = None
        mid_nodes = []
        nl = NodeLayout()
        for i, (k, v) in enumerate(df.values):
            if i == 0:
                from_node = k
                to_node = v
            # preserves order of midnodes
            if from_node in to_from.keys():
                # put at beginning of list
                mid_nodes.insert(0, from_node)
                from_node = to_from[from_node]
            if to_node in from_to.keys():
                # put at end of list
                mid_nodes.append(to_node)
                to_node = from_to[to_node]
        nl.edge = (from_node, to_node)
        nl.midnodes = mid_nodes
        nl.allnodes = None
        return nl


class RiverStemDetails:
    def __init__(self, comid, df, lookup_df):
        self.comid = comid
        self.df = df
        self.mainstem_level = float()
        self.mainstem_pathlength = float()
        self.edges = {
            'from': [],
            'to': [],
        }
        self.__mainstem_attributes()

        self.mainstem_branch_df = self.__get_primary_mainstem_branches(self.df)
        self.mainstem_bd = BranchDetails(self.__get_primary_mainstem_branches(self.df), self.df)
        self.first_order_branches = self.mainstem_bd.branch_map
        self.branch_map = dict()
        self.__find_all_branches(self.mainstem_bd)
        self.__create_edges_for_river_stem(self.branch_map, lookup_df)

    def __mainstem_attributes(self):
        mainstem_row = self.df[self.df.ComID == self.comid]
        mainstem_level = mainstem_row.StreamLeve.values[0]
        mainstem_pathlength = mainstem_row.Pathlength.values[0]
        mainstem_levelpathid = mainstem_row.LevelPathI.values[0]
        self.mainstem_level = mainstem_level
        self.mainstem_pathlength = mainstem_pathlength
        self.mainstem_levelpathid = mainstem_levelpathid

    # swanky bit of recursion here lets see how it works
    def __find_all_branches(self, BD):
        bm = BD.branch_map
        branch_map = {}
        for comid, branch_df in bm.items():
            if branch_df is not None:
                if len(branch_df) > 0:
                    next_bd = BranchDetails(branch_df, self.df)
                    bm[comid] = next_bd.branch_map
                    branch_map[comid] = bm[comid]
                    self.__find_all_branches(next_bd)
            else:
                bm[comid] = None
        self.branch_map = branch_map

    def __get_primary_mainstem_branches(self, df):
        mainstem_streamlevel = self.mainstem_level
        mainstem_levelpathid = self.mainstem_levelpathid
        first_order_branches = df[
            (df.StreamLeve == mainstem_streamlevel + 1.0)
            & (df.DnLevelPat == mainstem_levelpathid)
            ]
        return first_order_branches

    def __get_name_by_comid(self, comid, lookup_df):
        r = lookup_df[lookup_df.COMID == comid]
        name = r.GNIS_NAME.values[0]
        return name

    def __create_edges_for_river_stem(self, d, lookup_df):
        if d:
            for comid_to, value in d.items():
                if isinstance(value, dict):
                    for comid_from, next_br in value.items():
                        self.edges['to'].append(self.__get_name_by_comid(comid_to, lookup_df))
                        self.edges['from'].append(self.__get_name_by_comid(comid_from, lookup_df))
                        self.__create_edges_for_river_stem(next_br, lookup_df)
                else:
                    pass


class EdgeToNode:
    def __init__(self, datahandler=None, files=None):
        self.__read_files_fname = f'{Config.pickledir}/{Config.edgetonode}'
        try:
            with open(self.__read_files_fname, 'rb') as f:
                self.__dict__.update(pickle.load(f))
        except FileNotFoundError:
            if datahandler:
                if isinstance(datahandler, DataHandler.__class__):
                    self.datahandler = datahandler
            else:
                self.datahandler = DataHandler('data')

            # class attributes
            self.data = self.datahandler.read_df_from_dbf_file(['NHDFlowline.dbf', 'PlusFlowlineVAA.dbf'])
            data = copy.deepcopy(self.data)
            self.preparedData = PrepareData().prepare(data)
            self.vaa_df = self.data['PlusFlowlineVAA.dbf']
            self.nhdfl_df = self.data['NHDFlowline.dbf']
            self.riverbasins = None
            self.all_edges = {
                'from': [],
                'to': []
            }
            self.__build_river_basins_dicts()
            self.__build_edges()

    # TODO: Refactor this garbage for getting stem branches
    def __get_river_network_by_mainstem(self, terminal):
        df = self.vaa_df[self.vaa_df['TerminalFl'] == terminal]
        main_river_ids = df[['ComID', 'Hydroseq']].values
        dict_of_df_river_basins = dict()
        for comid, hydroseq in tqdm(main_river_ids):
            mainstem_basin_df = self.vaa_df[self.vaa_df['TerminalPa'] == hydroseq]
            mainstem_row = mainstem_basin_df[mainstem_basin_df.ComID == comid]
            if len(mainstem_row) > 0:
                rsd = RiverStemDetails(comid, mainstem_basin_df, self.preparedData)
                dict_of_df_river_basins[comid] = rsd
        self.riverbasins = dict_of_df_river_basins

    def __build_river_basins_dicts(self):
        self.__get_river_network_by_mainstem(1)

    def __build_edges(self):
        for mainstem_comid, rsd in self.riverbasins.items():
            self.all_edges['from'] += rsd.edges['from']
            self.all_edges['to'] += rsd.edges['to']

    def get_edges_as_list(self):
        return list(zip(self.all_edges['from'], self.all_edges['to']))

    def get_edges_as_df(self):
        return pd.DataFrame(self.all_edges)

    def save(self):
        with open(self.__read_files_fname, 'wb') as f:
            pickle.dump(self.__dict__, f)


def pretty(d, indent=0):
    for key, value in d.items():
        print('\t' * indent + str(key))
        if (isinstance(value, BranchDetails)) or (isinstance(value, RiverStemDetails)):
            pretty(value.branch_map, indent + 1)
        else:
            print('\t' * (indent + 1) + str(value))


class Network:
    def __init__(self, edges, force_update=False, init_args=None):
        self.__read_files_fname = f'{Config.pickledir}/{Config.network}'
        try:
            if force_update:
                self.init(**init_args)
            else:
                with open(self.__read_files_fname, 'rb') as f:
                    self.__dict__.update(pickle.load(f))
        except FileNotFoundError:
            self.edges = edges
            self.__G = None
            self.__layout = None

    def __make_Graph(self, graph_type):
        self.__G = graph_type()
        self.__G.add_edges_from(self.edges)

    def __make_layout(self, layout_type):
        self.__layout = layout_type(self.__G)

    def init(self, layout_type=nx.kamada_kawai_layout, graph_type=nx.Graph):
        print('making graph...')
        self.__make_Graph(graph_type)
        print('making layout...')
        self.__make_layout(layout_type)
        print('finished with graph and layout build')

    def get_Graph_and_Layout(self):
        """
        :return: tuple (Graph, Layout)
        """
        return self.__G, self.__layout

    def save(self):
        with open(self.__read_files_fname, 'wb') as f:
            pickle.dump(self.__dict__, f)


class NetworkViz:
    def __init__(self, network, **kwargs):
        self.__file_name = 'pickles/viz.html'
        self.__network = network
        self.G, self.pos = network.get_Graph_and_Layout()
        self.__viz = None

    def make_Viz(self, **kwargs):
        self.__viz = nxa.draw_networkx(
            self.G,
            pos=self.pos,
            **kwargs
        )

    def modify_viz(self, **kwargs):
        properties = kwargs['properties']
        self.__viz = self.__viz.properties(
            **properties
        ).interactive()

    def save_html(self):
        self.__viz.save(self.__file_name)

    def display(self):
        self.__viz.show()


if __name__ == '__main__':
    etn = EdgeToNode()
    # etn.save()
    edges = etn.get_edges_as_list()
    edges_df = etn.get_edges_as_df()
    edges_df.to_csv('pickles/edges.csv')
    # network = Network(edges)
    # # network.init()
    # # network.save()
    # viz = NetworkViz(network)
    # viz.make_Viz()
    # viz.modify_viz(
    #     properties={'width':3000,'height': 3000},
    #     # add more args
    # )
    # viz.save_html()
    # dfnhdfl = etn.data['NHDFlowline.dbf']
    # vaa = etn.data['PlusFlowlineVAA.dbf']
    # printt(dfnhdfl[dfnhdfl.COMID==15450474.0])
    # printt(vaa[vaa.ComID==15450474.0])
    # print(etn.data['NHDFlowline.dbf'].columns)
    # print(etn.data['PlusFlowlineVAA.dbf'].columns)
    # len(etn.data['NHDFlowline.dbf'].GNIS_NAME.unique())
    #
    # vaa columns
    # columns_in_vaa = ['ComID', 'Fdate', 'StreamLeve', 'StreamOrde', 'StreamCalc', 'FromNode',
    #                   'ToNode', 'Hydroseq', 'LevelPathI', 'Pathlength', 'TerminalPa',
    #                   'ArbolateSu', 'Divergence', 'StartFlag', 'TerminalFl', 'DnLevel',
    #                   'ThinnerCod', 'UpLevelPat', 'UpHydroseq', 'DnLevelPat', 'DnMinorHyd',
    #                   'DnDrainCou', 'DnHydroseq', 'FromMeas', 'ToMeas', 'ReachCode',
    #                   'LengthKM', 'Fcode', 'RtnDiv', 'OutDiv', 'DivEffect', 'VPUIn', 'VPUOut',
    #                   'AreaSqKM', 'TotDASqKM', 'DivDASqKM', 'Tidal', 'TOTMA', 'WBAreaType',
    #                   'PathTimeMA']
    #
    # columns_in_nhdfl = ['COMID', 'FDATE', 'RESOLUTION', 'GNIS_ID', 'GNIS_NAME', 'LENGTHKM',
    #                     'REACHCODE', 'FLOWDIR', 'WBAREACOMI', 'FTYPE', 'FCODE', 'SHAPE_LENG',
    #                     'ENABLED', 'GNIS_NBR']
