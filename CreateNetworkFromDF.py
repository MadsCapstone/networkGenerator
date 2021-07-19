import pickle
import pprint
from config import Config
import DataHandler as DH
import altair as alt
import networkx as nx
import nx_altair as nxa
from collections import defaultdict
from types import SimpleNamespace
import PrepareData
import pandas as pd
import time



class Network:
    def __init__(self, edges):
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
        with open('network', 'wb') as f:
            pickle.dump(self, f)


class NetworkViz:
    def __init__(self,network, **kwargs):
        self.__network = network
        network.init(**kwargs)
        self.G, self.pos = network.get_Graph_and_Layout()
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
    # ETN = EdgeToNode()
    # ETN.init()
    # ETN.save()

    node_connector = NodeConnector()
    node_connector.init()
    edges = node_connector.get_edges_as_tuple()

    network = Network(edges)
    viz = NetworkViz(network)
    viz.display()

    # node_connector.save_csv(filename='test')
    # node_connector.save_csv()

