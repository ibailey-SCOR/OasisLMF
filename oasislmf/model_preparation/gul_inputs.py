__all__ = [
    'get_gul_input_items',
    'write_coverages_file',
    'write_gul_input_files',
    'write_items_file',
    'write_complex_items_file'
]

import copy
import os
import sys
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from collections import OrderedDict

import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None

from ..utils.concurrency import (
    get_num_cpus,
    multithread,
    Task,
)
from ..utils.data import (
    factorize_array,
    get_dataframe,
    merge_dataframes,
    set_dataframe_column_dtypes,
)
from ..utils.defaults import (
    get_default_exposure_profile,
    OASIS_FILES_PREFIXES,
)
from ..utils.exceptions import OasisException
from ..utils.log import oasis_log
from ..utils.defaults import (
    COVERAGE_TYPES,
    FM_LEVELS,
    SOURCE_IDX,
)
from ..utils.path import as_path
from ..utils.profiles import (
    get_grouped_fm_profile_by_level_and_term_group,
    get_grouped_fm_terms_by_level_and_term_group,
    get_oed_hierarchy_terms,
)


@oasis_log
def get_gul_input_items(
    exposure_fp,
    keys_fp,
    exposure_profile=get_default_exposure_profile()
):
    """
    Generates and returns a Pandas dataframe of GUL input items.

    :param exposure_fp: Exposure file
    :type exposure_fp: str

    :param keys_fp: Keys file path
    :type keys_fp: str

    :param exposure_profile: Exposure profile
    :type exposure_profile: dict

    :return: GUL inputs dataframe
    :rtype: pandas.DataFrame

    :return: Exposure dataframe
    :rtype: pandas.DataFrame
    """
    # Get the exposure profile and a higher-level profile from that with terms
    # grouped by level and term group (and also term type)
    profile = get_grouped_fm_profile_by_level_and_term_group(exposure_profile=exposure_profile)

    if not profile:
        raise OasisException(
            'Source exposure profile is possibly missing FM term information: '
            'FM term definitions for TIV, limit, deductible, attachment and/or share.'
        )

    # Get another profile describing certain key ID columns in the exposure
    # file, namely loc. number, acc. number and portfolio number.
    hierarchy_terms = get_oed_hierarchy_terms(grouped_profile_by_level_and_term_group=profile)
    loc_num = hierarchy_terms['locid']
    acc_num = hierarchy_terms['accid']
    portfolio_num = hierarchy_terms['portid']
    cond_num = hierarchy_terms['condid']

    # Get the TIV column names and corresponding coverage types
    tiv_terms = OrderedDict({v['tiv']['CoverageTypeID']: v['tiv']['ProfileElementName'].lower() for k, v in profile[1].items()})

    # Define the cov. level and get the cov. level IL/FM terms
    cov_level = FM_LEVELS['site coverage']['id']
    cov_il_terms = get_grouped_fm_terms_by_level_and_term_group(grouped_profile_by_level_and_term_group=profile)[cov_level]

    tiv_and_cov_il_cols = [v for v in tiv_terms.values()] + [_v for v in cov_il_terms.values() for _v in v.values() if _v]

    # Set defaults and data types for the TIV and cov. level IL columns as
    # as well as the portfolio num. and cond. num. columns
    col_defaults = {t: (0.0 if t in tiv_and_cov_il_cols else 0) for t in tiv_and_cov_il_cols + [portfolio_num, cond_num]}
    col_dtypes = {
        **{t: 'float32' for t in tiv_and_cov_il_cols},
        **{t: 'uint32' for t in [cond_num]},
        **{t: 'str' for t in [loc_num, portfolio_num, acc_num]}
    }

    # Load the exposure and keys dataframes - set 32-bit numeric data types
    # for all numeric columns - and in the keys frame rename some columns
    # to align with underscored-naming convention
    exposure_df = get_dataframe(
        src_fp=exposure_fp,
        required_cols=(loc_num, acc_num, portfolio_num,),
        col_dtypes=col_dtypes,
        col_defaults=col_defaults,
        empty_data_error_msg='No data found in the source exposure (loc.) file',
        memory_map=True
    )

    col_dtypes = {
        'locid': 'str',
        'perilid': 'str',
        'coveragetypeid': 'uint32',
        'areaperilid': 'uint32',
        'vulnerabilityid': 'uint32',
        'modeldata': 'str'
    }
    keys_df = get_dataframe(
        src_fp=keys_fp,
        col_dtypes=col_dtypes,
        empty_data_error_msg='No keys found in the keys file',
        memory_map=True
    )
    keys_df.rename(
        columns={
            'locid': 'locnumber',
            'perilid': 'peril_id',
            'coveragetypeid': 'coverage_type_id',
            'areaperilid': 'areaperil_id',
            'vulnerabilityid': 'vulnerability_id',
            'modeldata': 'model_data'
        },
        inplace=True
    )

    # If the keys file relates to a complex/custom model then look for a
    # ``modeldata`` column in the keys file, and ignore the area peril
    # and vulnerability ID columns
    if 'model_data' in keys_df:
        keys_df['areaperil_id'] = keys_df['vulnerability_id'] = -1

    try:
        # Create the basic GUL inputs dataframe from merging the exposure and
        # keys dataframes on loc. number/loc. ID; filter out any rows with
        # zeros for TIVs for all coverage types, and replace any nulls in the
        # cond.num. and TIV columns with zeros
        exposure_df[SOURCE_IDX['loc']] = exposure_df.index.values
        gul_inputs_df = merge_dataframes(exposure_df, keys_df, on=loc_num, how='left')

        if gul_inputs_df.empty:
            raise OasisException(
                'Inner merge of the exposure file dataframe ({}) '
                'and the keys file dataframe ({}) on loc. number/loc. ID '
                'is empty - '
                'please check that the loc. number and loc. ID columns '
                'in the exposure and keys files respectively have a non-empty '
                'intersection'.format(exposure_fp, keys_fp)
            )

        del keys_df

        gul_inputs_df[cond_num].fillna(0, inplace=True)
        gul_inputs_df[cond_num] = gul_inputs_df[cond_num].astype('uint32')

        _tiv_cols = list(tiv_terms.values())
        gul_inputs_df = gul_inputs_df[(gul_inputs_df[_tiv_cols] != 0).any(axis=1)]
        gul_inputs_df.loc[:, _tiv_cols] = gul_inputs_df[_tiv_cols].where(gul_inputs_df.notnull(), 0.0)

        # Set default values and data types for BI coverage boolean, TIV, deductibles and limit
        gul_inputs_df = gul_inputs_df.assign(
            is_bi_coverage=False,
            tiv=0.0,
            deductible=0.0,
            deductible_min=0.0,
            deductible_max=0.0,
            limit=0.0
        )
        terms = ['tiv', 'deductible', 'deductible_min', 'deductible_max', 'limit']
        set_dataframe_column_dtypes(
            gul_inputs_df,
            {t: ('float32' if t in terms else 'bool') for t in ['is_bi_coverage'] + terms}
         )

        # Group the rows in the GUL inputs table by coverage type, and set the
        # IL terms (and BI coverage boolean) in each group and update the
        # corresponding frame section in the GUL inputs table
        for cov_type, cov_type_group in gul_inputs_df.groupby(by=['coverage_type_id'], sort=True):
            cov_type_group['is_bi_coverage'] = np.where(cov_type == COVERAGE_TYPES['bi']['id'], True, False)
            term_cols = [tiv_terms[cov_type]] + [(term_col or term) for term, term_col in cov_il_terms[cov_type].items() if term != 'share']
            cov_type_group.loc[:, term_cols] = cov_type_group.loc[:, term_cols].where(cov_type_group.notnull(), 0.0)
            cov_type_group.loc[:, terms] = cov_type_group.loc[:, term_cols].values
            cov_type_group = cov_type_group[(cov_type_group[['tiv']] != 0).any(axis=1)]
            if cov_type_group.empty:
                cov_type_group[terms] = 0.0
            else:
                cov_type_group['deductible'] = np.where(
                    (cov_type_group['deductible'] == 0) | (cov_type_group['deductible'] >= 1),
                    cov_type_group['deductible'],
                    cov_type_group['tiv'] * cov_type_group['deductible'],
                )
                cov_type_group['limit'] = np.where(
                    (cov_type_group['limit'] == 0) | (cov_type_group['limit'] >= 1),
                    cov_type_group['limit'],
                    cov_type_group['tiv'] * cov_type_group['limit'],
                )
            other_cov_type_term_cols = [v for k, v in tiv_terms.items() if k != cov_type] + [
                _v for k, v in cov_il_terms.items() for _v in v.values() if _v if k != 1
            ]
            cov_type_group.loc[:, other_cov_type_term_cols] = 0
            gul_inputs_df.loc[cov_type_group.index, ['is_bi_coverage'] + terms] = cov_type_group[['is_bi_coverage'] + terms]

        # Remove any rows with zeros in the ``tiv`` column and reset the index
        gul_inputs_df = gul_inputs_df[(gul_inputs_df[['tiv']] != 0).any(axis=1)].reset_index()

        # Set the group ID - group by loc. number
        gul_inputs_df['group_id'] = factorize_array(gul_inputs_df[loc_num].values)[0]
        gul_inputs_df['group_id'] = gul_inputs_df['group_id'].astype('uint32')

        # Set the item IDs and coverage IDs, and defaults for layer ID, agg. ID and summary and
        # summary set IDs
        item_ids = gul_inputs_df.index + 1
        gul_inputs_df = gul_inputs_df.assign(
            item_id=item_ids,
            coverage_id=item_ids,
            layer_id=1,
            agg_id=item_ids,
            summary_id=1,
            summaryset_id=1
        )
        set_dataframe_column_dtypes(
            gul_inputs_df,
            {t: 'uint32' for t in ['item_id', 'coverage_id', 'layer_id', 'agg_id', 'summary_id', 'summaryset_id']}
        )

        # Drop all unnecessary columns
        usecols = (
            [loc_num, acc_num, portfolio_num, cond_num] +
            ['tiv'] + tiv_and_cov_il_cols + terms +
            ['peril_id', 'coverage_type_id', 'areaperil_id', 'vulnerability_id'] +
            (['model_data'] if 'model_data' in gul_inputs_df else []) +
            ([SOURCE_IDX['loc']] if SOURCE_IDX['loc'] in gul_inputs_df else []) +
            ['is_bi_coverage', 'group_id', 'item_id', 'coverage_id', 'layer_id', 'agg_id', 'summary_id', 'summaryset_id']
        )
        gul_inputs_df.drop(
            [c for c in gul_inputs_df.columns if c not in usecols],
            axis=1,
            inplace=True
        )
    except (AttributeError, KeyError, IndexError, TypeError, ValueError) as e:
        raise OasisException from e

    return gul_inputs_df, exposure_df


@oasis_log
def write_complex_items_file(gul_inputs_df, complex_items_fp, chunksize=100000):
    """
    Writes a complex model items file.

    :param gul_inputs_df: GUL inputs dataframe
    :type gul_inputs_df: pandas.DataFrame

    :param complex_items_fp: Complex/custom model items file path
    :type complex_items_fp: str

    :return: Complex/custom model items file path
    :rtype: str
    """
    try:
        gul_inputs_df[['item_id', 'coverage_id', 'model_data', 'group_id']].drop_duplicates().to_csv(
            path_or_buf=complex_items_fp,
            encoding='utf-8',
            mode=('w' if os.path.exists(complex_items_fp) else 'a'),
            chunksize=chunksize,
            index=False
        )
    except (IOError, OSError) as e:
        raise OasisException from e


@oasis_log
def write_items_file(gul_inputs_df, items_fp, chunksize=100000):
    """
    Writes an items file.

    :param gul_inputs_df: GUL inputs dataframe
    :type gul_inputs_df: pandas.DataFrame

    :param items_fp: Items file path
    :type items_fp: str

    :return: Items file path
    :rtype: str
    """
    try:
        gul_inputs_df[['item_id', 'coverage_id', 'areaperil_id', 'vulnerability_id', 'group_id']].drop_duplicates().to_csv(
            path_or_buf=items_fp,
            encoding='utf-8',
            mode=('w' if os.path.exists(items_fp) else 'a'),
            chunksize=chunksize,
            index=False
        )
    except (IOError, OSError) as e:
        raise OasisException from e

    return items_fp


@oasis_log
def write_coverages_file(gul_inputs_df, coverages_fp, chunksize=100000):
    """
    Writes a coverages file.

    :param gul_inputs_df: GUL inputs dataframe
    :type gul_inputs_df: pandas.DataFrame

    :param coverages_fp: Coverages file path
    :type coverages_fp: str

    :return: Coverages file path
    :rtype: str
    """
    try:
        gul_inputs_df[['coverage_id', 'tiv']].drop_duplicates().to_csv(
            path_or_buf=coverages_fp,
            encoding='utf-8',
            mode=('w' if os.path.exists(coverages_fp) else 'a'),
            chunksize=chunksize,
            index=False
        )
    except (IOError, OSError) as e:
        raise OasisException from e

    return coverages_fp


@oasis_log
def write_gul_input_files(
    gul_inputs_df,
    target_dir,
    oasis_files_prefixes=copy.deepcopy(OASIS_FILES_PREFIXES['gul']),
    write_inputs_table_to_file=False
):
    """
    Writes the standard Oasis GUL input files to a target directory, using a
    pre-generated dataframe of GUL input items. The files written are
    ::

        items.csv
        coverages.csv

    and optionally a complex items file in case of a complex/custom model.

    :param gul_inputs_df: GUL inputs dataframe
    :type gul_inputs_df: pandas.DataFrame

    :param target_dir: Target directory in which to write the files
    :type target_dir: str

    :param oasis_files_prefixes: Oasis GUL input file name prefixes
    :param oasis_files_prefixes: dict

    :param write_inputs_table_to_file: Whether to write the GUL inputs table to file
    :param write_inputs_table_to_file: bool

    :return: GUL input files dict
    :rtype: dict
    """
    # Clean the target directory path
    target_dir = as_path(target_dir, 'Target IL input files directory', is_dir=True, preexists=False)

    # Set chunk size for writing the CSV files - default is 100K
    chunksize = min(2 * 10**5, len(gul_inputs_df))

    # A debugging option
    if write_inputs_table_to_file:
        gul_inputs_df.to_csv(
            path_or_buf=os.path.join(target_dir, 'gul_inputs.csv'),
            index=False,
            encoding='utf-8',
            chunksize=chunksize
        )

    # If no complex model data present then remove the corresponding file
    # name from the files prefixes dict, which is used for writing the
    # GUl input files
    if 'model_data' not in gul_inputs_df:
        if oasis_files_prefixes.get('complex_items'):
            oasis_files_prefixes.pop('complex_items')

    # A dict of GUL input file names and file paths
    gul_input_files = {
        fn: os.path.join(target_dir, '{}.csv'.format(oasis_files_prefixes[fn]))
        for fn in oasis_files_prefixes
    }

    # GUL input file writers have the same filename prefixes as the input files
    # and we use this property to dynamically retrieve the methods from this
    # module by name - this is it is necessary here to have the module object
    this_module = sys.modules[__name__]

    # Get the physical CPU count on the instance - this is used as one of the
    # conditions to test for when deciding whether to write the input files
    # using multiple threads
    cpu_count = get_num_cpus()

    # If the GUL inputs size doesn't exceed the chunk size, or there are
    # sufficient physical CPUs to cover the number of input files to be written,
    # then use multiple threads to write the files, otherwise write them
    # serially
    if len(gul_inputs_df) <= chunksize or cpu_count >= len(gul_input_files):
        tasks = (
            Task(
                getattr(this_module, 'write_{}_file'.format(fn)),
                args=(gul_inputs_df.copy(deep=True), gul_input_files[fn], chunksize,),
                key=fn
            )
            for fn in gul_input_files
        )
        num_ps = min(len(gul_input_files), cpu_count)
        for _, _ in multithread(tasks, pool_size=num_ps):
            pass
    else:
        for fn, fp in gul_input_files.items():
            getattr(this_module, 'write_{}_file'.format(fn))(gul_inputs_df, fp, chunksize)

    return gul_input_files
