"""Prepare data for this project

    path file (.csv):
        should merge the data in order by timestamp
        make sure the fps of all videos are same
        structure: path/*/paths.csv

    temp file (.MAT):
        joint the frame and add palette for visualization
        structure: temp/*.MAT

"""
import argparse
from pathlib import Path

import logme
from src.preprocess import get_aggregate_path


def argparser():
    """CLI interface to pass argument"""
    parser = argparse.ArgumentParser(description='Prepare data for this project')
    subparser = parser.add_subparsers(dest='cmd')

    # set subparser for path preprocess
    path_parser = subparser.add_parser('path', \
        description='concat multiple paths as one experiment path')
    path_parser.add_argument('--dir', dest='dir', required=True, \
        help='a dir contains multi-videos dir')
    path_parser.add_argument('--reverse', dest='reverse', action='store_true', \
        help='reverse the order of video list')
    path_parser.set_defaults(reverse=False)
    return parser

@logme.log
def main(args: argparse.Namespace, logger=None):
    """Preprocess for different target
    Arguments:
        args {argparse.Namespace} -- parse argument from CLI

    Keyword Arguments:
        logger {[type]} -- logger from (default: {None})

    # aggregate multiple paths in a experiment
    $ python3 preprocess.py path --dir data/demo/path
    """

    logger.info(args)

    if args.cmd == 'path':
        path_root_dir = Path(args.dir)
        assert path_root_dir.exists()
        path_videos_dir = [d for d in path_root_dir.iterdir() if d.is_dir()]
        path_videos_dir = [str(next(d.glob('*.csv'))) for d in path_videos_dir]
        path_videos_dir = sorted(path_videos_dir)
        df_paths = get_aggregate_path(*path_videos_dir[:2], reverse=args.reverse)
        # df_paths.to_csv('output/experiment_paths.csv', index=False)

if __name__ == '__main__':
    main(argparser().parse_args())
