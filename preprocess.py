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
import yaml

from src.preprocess import get_aggregate_path, get_downgrade_fps_path
from src.utils import profile, check_exist_type


def argparser():
    """CLI interface to pass argument"""
    parser = argparse.ArgumentParser(description='Prepare data for this project')
    parser.add_argument('-c', '--config', dest='config', default='config.yaml', \
        help='config file (.yaml)')
    subparser = parser.add_subparsers(dest='cmd')

    # set subparser for path aggregate (multiple .csv -> one .csv)
    # alignment if given fps (thermal fps default value should be 1)
    path_parser = subparser.add_parser('path', \
        description='concat multiple paths as one experiment path')
    path_parser.add_argument('--dir', dest='dir', required=True, \
        help='a dir contains multi-videos dir')
    path_parser.add_argument('--reverse', dest='reverse', action='store_true', \
        help='reverse the order of video list')
    path_parser.add_argument('--no-save', dest='save', action='store_false', \
        help='save path preprocess file')
    path_parser.set_defaults(reverse=False, save=True)

    return parser

@logme.log
@profile
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
    with open(args.config, 'r') as yaml_file:
        config = yaml.load(yaml_file)
        config = config['preprocess']

    if args.cmd == 'path':
        path_root_dir = Path(args.dir)
        assert path_root_dir.exists()
        path_videos_dir = [d for d in path_root_dir.iterdir() if d.is_dir()]
        path_videos_dir = [str(next(d.glob('*.csv'))) for d in path_videos_dir]
        path_videos_dir = sorted(path_videos_dir)
        df_paths = get_aggregate_path(*path_videos_dir[:2], reverse=args.reverse)

        # get_aggregate_path() will guarantee the fps is unique and in the float64 type
        video_fps = df_paths.video_fps.unique()[0]
        if check_exist_type(config['thermal_fps'], int) and config['thermal_fps'] < video_fps:
            block_threshold = 1
            if check_exist_type(config['block_length'], int):
                block_threshold = config['block_length']
            df_paths = get_downgrade_fps_path(
                df_paths, fps=config['thermal_fps'], block_threshold=block_threshold)
        print(df_paths.head())

        if args.save:
            savepath = str(path_root_dir / 'aggr_path.csv')
            df_paths.to_csv(savepath, index=False)

if __name__ == '__main__':
    main(argparser().parse_args())
