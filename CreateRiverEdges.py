from DataHandler import DataHandler


class MakeEdges:
    def __init__(self):
        dh = DataHandler('data')
        data = dh.read_df_from_dbf_file(['PlusFlow.dbf', 'NHDFlowline.dbf'])
        self.flowline_df = data['NHDFlowline.dbf']
        print(self.flowline_df.columns)
        self.flow_df = data['PlusFlow.dbf']
        print(self.flow_df.columns)
        # getting rid of self connections by comid first
        # also dropping same level path
        self.flow_df_parsed = self.__drop_self_connections(self.flow_df)
        self.__zip_com_id_name = zip(self.flowline_df.COMID,self.flowline_df.GNIS_NAME)
        self.com_id_name_list = list(self.__zip_com_id_name)
        self.com_id_name_dict = dict(self.com_id_name_list)
        self.edges_unrefined = list(zip(self.flow_df_parsed.FROMCOMID, self.flow_df_parsed.TOCOMID))
        self.unnamed_comid = set()
        self.named_unnamed_edges_dict = dict()
        self.edges_by_comid = []
        self.edges = []
        self.named_edges_with_unnamed = []
        self.named_edges_no_unnamed = []
        self.edges_not_found = []
        self.__make_edges()



    def __make_edges(self):
        for f_id, t_id in self.edges_unrefined:
            try:
                from_name = self.com_id_name_dict[f_id]
                to_name = self.com_id_name_dict[t_id]
                if from_name != to_name:
                    self.edges_by_comid.append((f_id, t_id))
                    self.named_edges_with_unnamed.append((from_name, to_name))
                    if (from_name == '') or (to_name == ''):
                        if from_name == '':
                            self.unnamed_comid.add(f_id)
                            name_from_ = self.__name_unnamed_edges(f_id)
                            self.edges.append((name_from_, to_name))
                        if to_name == '':
                            self.unnamed_comid.add(t_id)
                            name_to_ = self.__name_unnamed_edges(t_id)
                            self.edges.append((from_name, name_to_))
                    else:
                        self.named_edges_no_unnamed.append((from_name, to_name))
                else:
                    if (from_name == '') and (to_name == ''):
                        name_from_ = self.__name_unnamed_edges(f_id)
                        name_to_ = self.__name_unnamed_edges(t_id)
                        self.edges.append((name_from_, name_to_))
                    else:
                        pass

            except KeyError as e:
                self.edges_not_found.append(e.args)
                pass

    def __drop_self_connections(self, df):
        df = df[df.FROMCOMID != df.TOCOMID]
        df = df[df.FROMLVLPAT != df.TOLVLPAT]
        return df

    def __name_unnamed_edges(self, comid):
        name = f'Unnamed_{comid}'
        self.named_unnamed_edges_dict[comid] = name
        return name

    def __handle_two_unnamed_edge(self, from_name, from_id, to_name, to_id):
        pass

    def get_edges_as_df(self):
        pass

    def get_edges_as_list(self):
        pass

if __name__ == '__main__':
    me = MakeEdges()
    print(len(me.named_edges_no_unnamed))