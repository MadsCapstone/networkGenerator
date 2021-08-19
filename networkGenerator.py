import sys
import DataHandler
import CreateNetworkFromDF
import argparse
import CreateRiverEdges
import pandas as pd

def make_edges():
    make_edges = CreateRiverEdges.MakeEdges()
    return make_edges

def retrieve_output_type(make_edges, type):
    def make_df_from_edges(edges):
        return pd.DataFrame(columns=['from', 'to'], data=edges)

    df = None
    if type == 'named':
        df = make_df_from_edges(make_edges.named_edges_with_unnamed)

    if type == 'namedonly':
        df = make_df_from_edges(make_edges.named_edges_no_unnamed)

    if type == 'all':
        df = make_df_from_edges(make_edges.named_edges_with_placeholder_name)

    if type == 'bycomid':
        df = make_df_from_edges(make_edges.edges_by_comid)

    return df

def save_output(fname, df):
    df.to_csv(f'output/{fname}')

if __name__ == '__main__':
    help_type = f"""
        Example type outputs:
        \n
        --type [namedonly]\n
        [('Martin Branch', 'Stoney Brook'), ('McCarty River', 'Saint Louis River'), ('Mud Hen Creek', 'Saint Louis River'), ('Sand Creek', 'Saint Louis River'), ('Shiver Creek', 'North Branch Whiteface River'), ('Spring Mine Creek', 'Embarrass River'), ('Stony Creek', 'Saint Louis River'), ('West Branch Bug Creek', 'Bug Creek'), ('Red River', 'Saint Louis River'), ('Silver Creek', 'Saint Louis River'), ('Otter Creek', 'Saint Louis River')]
        \n
        --type [named]\n
        [('Joula Creek', 'Floodwood River'), ('Little Whiteface River', 'Whiteface River'), ('', 'Little Whiteface River'), ('Little Whiteface River', ''), ('', 'Little Whiteface River'), ('Little Whiteface River', ''), ('Little Whiteface River', ''), ('', 'Little Whiteface River'), ('Little Whiteface River', ''), ('Little Whiteface River', ''), ('', 'Little Whiteface River'), ('Little Whiteface River', ''), ('Little Whiteface River', ''), ('Little Whiteface River', ''), ('Little Whiteface River', '')]
        \n
        --type [all]\n
        [('Unnamed_1770694', 'Little Whiteface River'), ('Unnamed_1770694', 'Unnamed_1770686'), ('Little Whiteface River', 'Unnamed_1770694')]
        \n
        --type [bycomid] \n
        [(1777620, 1777628), (1776592, 1776600), (1771946, 1771952), (1777340, 1776798), (1772158, 1772162)]
    """
    parser = argparse.ArgumentParser(description='build the edges to use and such')
    parser.add_argument('--make_edges', type=str, nargs='*', help='execute script to make edges if not provided nothing will happen', required=True)
    parser.add_argument('--output', type=str, nargs=1, help='directory to save csv, this defaults to output/[file name you provide]', required=True)
    parser.add_argument('--type', type=str, choices=['named','namedonly','bycomid', 'all'], nargs=1, help=help_type, required=True)

    args = parser.parse_args()
    if 'make_edges' in args:
        me = make_edges()
        edges_df = retrieve_output_type(me, args.type[0])

        save_output(args.output[0], edges_df)
    else:
        print("nothing is going to happen with out providing args")
        print("type python networkGenerator.py --help")
        sys.exit(1)







