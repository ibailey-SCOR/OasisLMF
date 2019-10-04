
import warnings
from collections import OrderedDict

from .exceptions import OasisException

from .profiles import (
    get_fm_terms_oed_columns,
    get_grouped_fm_profile_by_level_and_term_group,
    get_grouped_fm_terms_by_level_and_term_group,
    get_oed_hierarchy,
)

from .fm import SUPPORTED_FM_LEVELS
from .coverages import SUPPORTED_COVERAGE_TYPES
from .defaults import get_default_exposure_profile
from .data import get_dataframe
from .data import get_ids


def read_exposure_df(exposure_fp, exposure_profile=get_default_exposure_profile()):
    """
    Generates and returns a Pandas dataframe of GUL input items.

    :param exposure_fp: Exposure file
    :type exposure_fp: str

    :param exposure_profile: Exposure profile
    :type exposure_profile: dict

    :return: Exposure dataframe
    :rtype: pandas.DataFrame
    """

    # Get the grouped exposure profile - this describes the financial terms to
    # to be found in the source exposure file, which are for the following
    # FM levels: site coverage (# 1), site pd (# 2), site all (# 3). It also
    # describes the OED hierarchy terms present in the exposure file, namely
    # portfolio num., acc. num., loc. num., and cond. num.
    profile = get_grouped_fm_profile_by_level_and_term_group(exposure_profile=exposure_profile)

    if not profile:
        raise OasisException(
            'Source exposure profile is possibly missing FM term information: '
            'FM term definitions for TIV, limit, deductible, attachment and/or share.'
        )

    # Get the OED hierarchy terms profile - this defines the column names for loc.
    # ID, acc. ID, policy no. and portfolio no., as used in the source exposure
    # and accounts files. This is to ensure that the method never makes hard
    # coded references to the corresponding columns in the source files, as
    # that would mean that changes to these column names in the source files
    # may break the method
    oed_hierarchy = get_oed_hierarchy(exposure_profile=exposure_profile)
    loc_num = oed_hierarchy['locnum']['ProfileElementName'].lower()
    acc_num = oed_hierarchy['accnum']['ProfileElementName'].lower()
    portfolio_num = oed_hierarchy['portnum']['ProfileElementName'].lower()
    cond_num = oed_hierarchy['condnum']['ProfileElementName'].lower()

    # The (site) coverage FM level ID (# 1 in the OED FM levels hierarchy)
    cov_level_id = SUPPORTED_FM_LEVELS['site coverage']['id']

    # Get the TIV column names and corresponding coverage types
    tiv_terms = OrderedDict({v['tiv']['CoverageTypeID']: v['tiv']['ProfileElementName'].lower()
                             for k, v in profile[SUPPORTED_FM_LEVELS['site coverage']['id']].items()})
    tiv_cols = list(tiv_terms.values())

    # Get the list of coverage type IDs - financial terms for the coverage
    # level are grouped by coverage type ID in the grouped version of the
    # exposure profile (profile of the financial terms sourced from the
    # source exposure file)
    cov_types = [v['id'] for v in SUPPORTED_COVERAGE_TYPES.values()]

    # Get the FM terms profile (this is a simplfied view of the main grouped
    # profile, containing only information about the financial terms), and
    # the list of OED colum names for the financial terms for the site coverage
    # (# 1 ) FM level
    fm_terms = get_grouped_fm_terms_by_level_and_term_group(grouped_profile_by_level_and_term_group=profile)
    terms_floats = ['deductible', 'deductible_min', 'deductible_max', 'limit']
    terms_ints = ['ded_code', 'ded_type', 'lim_code', 'lim_type']
    terms = terms_floats + terms_ints
    term_cols_floats = get_fm_terms_oed_columns(
        fm_terms,
        levels=['site coverage'],
        term_group_ids=cov_types,
        terms=terms_floats
    )
    term_cols_ints = get_fm_terms_oed_columns(
        fm_terms,
        levels=['site coverage'],
        term_group_ids=cov_types,
        terms=terms_ints
    )
    term_cols = term_cols_floats + term_cols_ints

    # Load the exposure dataframes - set 64-bit float data types
    # for all real number columns - and in the keys frame rename some columns
    # to align with underscored-naming convention;
    # Set defaults and data types for the TIV and cov. level IL columns as
    # as well as the portfolio num. and cond. num. columns
    defaults = {
        **{t: 0.0 for t in tiv_cols + term_cols_floats},
        **{t: 0 for t in term_cols_ints},
        **{cond_num: 0},
        **{portfolio_num: '1'}
    }
    dtypes = {
        **{t: 'float64' for t in tiv_cols + term_cols_floats},
        **{t: 'uint8' for t in term_cols_ints},
        **{t: 'uint16' for t in [cond_num]},
        **{t: 'str' for t in [loc_num, portfolio_num, acc_num]},
        **{t: 'uint32' for t in ['loc_id']}
    }

    exposure_df = get_dataframe(
        src_fp=exposure_fp,
        required_cols=(loc_num, acc_num, portfolio_num,),
        col_dtypes=dtypes,
        col_defaults=defaults,
        empty_data_error_msg='No data found in the source exposure (loc.) file',
        memory_map=True
    )

    # Set the `loc_id` column in the exposure dataframe to identify locations uniquely with respect
    # to portfolios and portfolio accounts. This loc_id must be consistent with the keys file
    if 'loc_id' not in exposure_df:
        if 'locid' in exposure_df.columns:
            warnings.warn('loc_id field not in loc file... using locid')
            exposure_df.rename(columns={'locid': 'loc_id'}, inplace=True)
        else:
            warnings.warn('loc_id field not in loc file... building')
            exposure_df['loc_id'] = get_ids(exposure_df, [portfolio_num, acc_num, loc_num])

    # Check the loc_id is a consistent index
    if exposure_df.loc_id.nunique() != len(exposure_df):
        warnings.warn("Non-unique loc_id entries found.")

    return exposure_df

