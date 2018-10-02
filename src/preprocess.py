"""preprocess.py"""
import datetime

import logme
import pandas as pd


def _add_group_id(df: pd.DataFrame, groupby_cols: list, gid_colname: str='gid') -> pd.DataFrame:
    """[summary] add group index by given col

    Arguments:
        df {pd.DataFrame} -- target dataframe to groupby and add group id
        groupby_cols {tuple} -- reference column to groupby

    Keyword Arguments:
        gid_colname {str} -- group index colname (default: {'gid'})
    
    Returns:
        pd.DataFrame -- the DataFrame appended the group id
    """
    df_group = df.groupby(groupby_cols).apply(lambda group: pd.Series({
        'group_length': group.shape[0]
    })).reset_index()
    df_group[gid_colname] = df_group.index
    df_merge = pd.merge(df, df_group, how='outer', on=groupby_cols)
    df_merge['group_length'] = df_merge['group_length'].fillna(-1)
    df_merge[gid_colname] = df_merge[gid_colname].fillna(-1)
    df_merge['group_length'] = df_merge['group_length'].astype(int)
    df_merge[gid_colname] = df_merge[gid_colname].astype(int)
    return df_merge

def ms_to_hmsf(ms: float) -> str:
    """[summary] convert milliseconds to hms+microseconds

    Arguments:
        ms {float} -- milliseconds

    Returns:
        {str} -- hms + microseconds
    """

    delta = datetime.timedelta(microseconds=ms*1000)
    hmsf = (datetime.datetime.min + delta).strftime('%H:%M:%S.%f')
    return hmsf

@logme.log
def get_aggregate_path(*path_files, reverse: bool=False, logger=None) -> pd.DataFrame:
    """[summary] aggregate data by rules

    - `video_fps` should be all the same
    - `frame_idx` should be increment by video order
    - recalc `video_nframes`, `timestamp_ms` and `block_idx`
    - calc `timestamp_hmsf` by recalc `timestamp_ms`

    timestamp_ms: milliseconds (1e-3)
    timestamp_hmsf: hour:minute:second.microseconds (1e-6)

    Arguments:
        *path_files {str} -- multiple path file (.csv)

    Keyword Arguments:
        reverse {bool} -- reverse order of video list (default: {False})
        logger {[type]} -- logger (default: {None})

    Returns:
        {pd.DataFrame} -- aggregate result
    """

    df_paths = None
    path_files = sorted(path_files)
    if reverse:
        path_files = path_files[::-1]
    for vid, video_path_file in enumerate(path_files):
        df_one_video_path = pd.read_csv(video_path_file)
        if df_paths is None:
            assert len(df_one_video_path.video_fps.unique()) == 1
            df_one_video_path = _add_group_id(df_one_video_path, ['video_name', 'block_idx'], 'gid')
            df_paths = df_one_video_path
            logger.debug('#%d video, fps=%.2f', vid, df_one_video_path.video_fps.unique()[0])
            continue

        # check fps
        assert df_paths.video_fps.unique()[0] == df_one_video_path.video_fps.unique()[0]
        logger.debug('#%d video, fps=%.2f', vid, df_one_video_path.video_fps.unique()[0])

        # recalc frame_idx
        assert len(df_paths.video_nframes.unique()) == 1
        base_frame_idx = df_paths.video_nframes.unique()[0]
        df_one_video_path['frame_idx'] += base_frame_idx
        logger.debug('#%d video, add the base frame idx %d', vid, base_frame_idx)

        # recalc block_idx (action clip index)
        base_block_idx = max(df_paths.block_idx.unique())
        df_one_video_path = _add_group_id(df_one_video_path, ['video_name', 'block_idx'], 'gid')
        df_one_video_path.block_idx += base_block_idx

        # concat the prepared data
        df_paths = pd.concat([df_paths, df_one_video_path])
        df_paths.video_nframes = sum(df_paths.video_nframes.unique())
        logger.debug('#%d video, total nframes %d', vid, df_paths.video_nframes.unique()[0])
    df_paths.block_idx = df_paths.gid
    df_paths.drop(columns=['gid', 'group_length'], inplace=True)

    # recalc timestamp
    df_paths.timestamp_ms = df_paths.frame_idx / df_paths.video_fps * 1000
    timestamp_hmsf = df_paths.timestamp_ms.apply(ms_to_hmsf)
    col_timestamp_ms_idx = df_paths.columns.get_loc('timestamp_ms')
    df_paths.insert(col_timestamp_ms_idx, 'timestamp_hmsf', timestamp_hmsf)
    return df_paths
