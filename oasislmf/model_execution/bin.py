"""
    Python utilities used for setting up the structure of the run directory
    in which to prepare the inputs to run a model or generate deterministic
    losses, and store the outputs.
"""
__all__ = [
    'check_binary_tar_file',
    'check_conversion_tools',
    'check_inputs_directory',
    'cleanup_bin_directory',
    'create_binary_tar_file',
    'csv_to_bin',
    'prepare_run_directory',
    'prepare_run_inputs',
    'move_input_files',
    'copy_static_files'
]

import filecmp
import glob
import logging
import os
import errno
import re
import shutil
import shutilwhich
import tarfile
import warnings

from itertools import chain

from pathlib2 import Path
import pandas as pd

from ..utils.exceptions import OasisException
from ..utils.log import oasis_log
from ..utils.file_conversion import csvfile_to_bin
from .files import TAR_FILE, INPUT_FILES, GUL_INPUT_FILES, IL_INPUT_FILES

@oasis_log
def prepare_run_directory(
    run_dir,
    analysis_settings_fp,
    inputs_archive=None,
    user_data_dir=None,
    ri=False,
):
    """
    Ensures that the model run directory has the correct folder structure in
    order for the model run script (ktools) to be executed. Without the RI
    flag the model run directory will have the following structure

    ::

        <run_directory>
        |-- fifo/
        |-- input/
        |   `-- csv/
        |-- output/
        |-- static/
        |-- work/
        |-- analysis_settings.json
        `-- run_ktools.sh


    where the direct GUL and/or FM input files exist in the ``input/csv``
    subfolder and the corresponding binaries exist in the ``input`` subfolder.

    With the RI flag the model run directory has the following structure

    ::
        <run_directory>
        |-- fifo
        |-- input
        |-- RI_1
        |-- RI_2
        |-- ...
        |-- output
        |-- static
        |-- work
        |-- ri_layers.json
        |-- analysis_settings.json
        `-- run_ktools.sh

    where the direct GUL and/or FM input files, and the corresponding binaries
    exist in the ``input`` subfolder, and the RI layer input files and binaries
    exist in the ``RI`` prefixed subfolders.

    If any subfolders are missing they are created.

    Optionally, if the path to the analysis settings JSON file is provided
    then it is copied to the base of the run directory.

    :param run_dir: the model run directory
    :type run_dir: str

    :param ri: Boolean flag for RI mode
    :type ri: bool

    :param analysis_settings_fp: analysis settings JSON file path
    :type analysis_settings_fp: str

    :param: user_data_dir: path to a directory containing additional user-supplied model data
    :type user_data_dir: str
    """
    try:
        # Create folders
        for subdir in ['fifo', 'output', 'static', 'work']:
            Path(run_dir, subdir).mkdir(parents=True, exist_ok=True)

        if not inputs_archive:
            Path(run_dir, 'input').mkdir(parents=True, exist_ok=True)
        else:
            with tarfile.open(inputs_archive) as input_tarfile:
                p = os.path.join(run_dir, 'input') if not ri else os.path.join(run_dir)
                input_tarfile.extractall(path=p)

        # Copy analysis settings into the run folder
        dst = os.path.join(run_dir, 'analysis_settings.json')
        shutil.copy(analysis_settings_fp, dst) if not (os.path.exists(dst) and
                                                       filecmp.cmp(analysis_settings_fp, dst, shallow=False)) else None

        # Copy user data
        oasis_dst_fp = os.path.join(run_dir, 'input')
        if user_data_dir and os.path.exists(user_data_dir):
            for path in glob.glob(os.path.join(user_data_dir, '*')):
                fn = os.path.basename(path)
                try:
                    if os.name == 'nt':
                        shutil.copy(path, os.path.join(oasis_dst_fp, fn))
                    else:
                        os.symlink(path, os.path.join(oasis_dst_fp, fn))
                except Exception:
                    shutil.copytree(user_data_dir, os.path.join(oasis_dst_fp, fn))

    except OSError as e:
        raise OasisException from e


def link_or_copy_file(filename, source_folder, destination_folder):
    """Try to create a symbolic link for a file, otherwise copy it

    filename: (str) name of the file without path including suffix

    source_folder: (str) folder from which to link/copy

    desintation_folder: (str) folder to which we link or copy

    """

    # Check if the file exists
    if not os.path.exists(os.path.join(source_folder, filename)):
        raise OasisException("Source file {} doesn't exist".format(
            os.path.join(source_folder, filename)))

    # Make a soft link of the file in the new folder
    try:
        # Use symbolic link if we can
        os.symlink(
            os.path.join(source_folder, filename),
            os.path.join(destination_folder, filename))
        print("\tLinking {} from {}".format(filename, source_folder))

    except OSError as why:
        if why.errno == errno.EEXIST:
            # Check if the link already exists, then do nothing
            print("\tNot linking {} because destn file exists".format(filename))
        else:
            # Otherwise try to copy the file (probably necessary on windows)
            try:
                print("\tCopying {} from {}".format(filename, source_folder))
                shutil.copy2(
                    os.path.join(source_folder, filename),
                    os.path.join(destination_folder, filename))

            except OSError as e:
                raise OasisException from e


def copy_static_files(run_dir, model_data_fp, analysis_settings):
    """Link or copy files into the static folder
    """

    # Force use of the full path for the source
    model_data_fp = os.path.abspath(model_data_fp)

    # Start with list of files that are always required
    static_files = ['footprint', 'vulnerability', 'damage_bin_dict']

    # Check if random is required
    if 'model_settings' in analysis_settings:
        if (('use_random_number_file'in analysis_settings['model_settings']) and
            (analysis_settings['model_settings']['use_random_number_file'])):
         static_files.append('random')

    # Add the bin suffix
    static_files = [f + ".bin" for f in static_files]
    static_files.append("footprint.idx")

    # Add the non-keys dict files if they exist
    optional_files = ['event_dict.csv', 'intensity_bin_dict.csv']
    for fnm in optional_files:
        if os.path.exists(os.path.join(model_data_fp, fnm)):
            static_files.append(fnm)

    # Get the destination folder path.
    model_data_dst_fp = os.path.join(run_dir, 'static')

    # Loop through each file and copy it
    for fnm in static_files:
        link_or_copy_file(fnm, model_data_fp, model_data_dst_fp)


def move_input_files(run_dir, oasis_src_fp, analysis_settings):
    """Move input files into the input folder"""
    # Move input files into the input folder

    oasis_dst_fp = os.path.join(run_dir, 'input')
    try:
        for p in os.listdir(oasis_src_fp):
            src = os.path.join(oasis_src_fp, p)

            # Don't copy an tar or tar.gz files
            if src.endswith('.tar') or src.endswith('.tar.gz'):
                continue

            dst = os.path.join(oasis_dst_fp, p)

            # Check if it is a reinsurance folder
            if not (re.match(r'RI_\d+$', p) or p == 'ri_layers.json'):
                # Make a copy if it is not a reinsurance folder
                shutil.copy2(src, oasis_dst_fp) if not (os.path.exists(dst) and filecmp.cmp(src, dst)) else None
            else:
                shutil.move(src, run_dir)

    except OSError as e:
        raise OasisException from e


def copy_run_input_file(filename, setting_val, input_fp, modeldata_fp):

    # Check if the file exists already in input folder with the right name
    if setting_val is None:
        for suffix in ['.bin', '.csv']:
            if os.path.exists(input_fp, filename + suffix):
                return

    # Otherwise we will have to copy it from either a different filename or a different folder
    if setting_val is not None:
        srcfilename = "{}_{}".format(filename, setting_val)
        search_folders = [input_fp, modeldata_fp]
    else:
        srcfilename = filename
        search_folders = [modeldata_fp]

    for folder in search_folders:
        for suffix in [".bin", ".csv"]:
            src_fp = os.path.join(folder, srcfilename + suffix)
            dest_fp = os.path.join(input_fp, filename + suffix)
            if os.path.exists(src_fp):
                shutil.copy2(src_fp, dest_fp)
                return

    # If we get here, the file has not been found
    raise OasisException("File {} was not found as csv or bin in {} or {}".format(
        srcfilename, input_fp, modeldata_fp))


def list_required_run_inputs(analysis_settings):
    """Based on analysis settings, return a list of model input data files (not exposure related) that will be
    required.

    Returns a list of filenames as expected by ktools, without any file suffixes.
    """

    # Events file is always required
    input_files = ['events']

    # Check if return periods are required
    is_rp = False

    # Check if occurrence is required
    is_occ = False

    for summary_type in ['gul_summaries', 'il_summaries', 'ri_summaries']:
        if summary_type not in analysis_settings:
            continue

        # Loop through each of the summary levels requested in the analysis settings
        for summary in analysis_settings[summary_type]:
            if 'aalcalc' in summary and summary['aalcalc'] is True:
                is_occ = True
            if 'leccalc' in summary and summary['leccalc'] is True:
                is_occ = True
                if ('return_period_file' in summary['leccalc'] and
                    summary['leccalc']['return_period_file'] is True):
                    is_rp = True

    if is_occ:
        input_files.append('occurrence')

    if is_rp:
        input_files.append('returnperiods')

    # TODO: periods

    return input_files


@oasis_log
def prepare_run_inputs(analysis_settings, run_dir, ri=False, model_data_fp=None):
    """
    Sets up binary files in the model inputs directory.

    :param analysis_settings: model analysis settings dict
    :type analysis_settings: dict

    :param run_dir: model run directory
    :type run_dir: str
    """

    # Get a list of the input files that are needed
    file_list = list_required_run_inputs(analysis_settings)

    # Get model settings, which has any filename identifier for specific set of events/occurrences
    model_settings = analysis_settings.get('model_settings', {})

    # Get the destination path
    destn_path = os.path.join(run_dir, 'input')

    try:

        if 'events' in file_list:
            setting_val = str(model_settings.get('event_set')).replace(' ', '_').lower()
            copy_run_input_file('events', setting_val, destn_path, model_data_fp)

        if 'returnperiods' in file_list:
            copy_run_input_file('returnperiods', None, destn_path, model_data_fp)
        
        if 'occurrence' in file_list:
            setting_val = str(model_settings.get('event_occurrence_id')).replace(' ', '_').lower()
            copy_run_input_file('occurrence', setting_val, destn_path, model_data_fp)

        if 'periods' in file_list:
            copy_run_input_file('periods', setting_val, destn_path, model_data_fp)

    except (OSError, IOError) as e:
        raise OasisException from e

    return file_list

@oasis_log
def check_inputs_directory(directory_to_check, il=False, ri=False, check_binaries=True):
    """
    Check that all the required files are present in the directory.

    :param directory_to_check: directory containing the CSV files
    :type directory_to_check: string

    :param il: check insuured loss files
    :type il: bool

    :param il: check resinsurance sub-folders
    :type il: bool

    :param check_binaries: check binary files are not present
    :type check_binaries: bool
    """
    # Check the top level directory, that containes the core files and any direct FM files
    _check_each_inputs_directory(directory_to_check, il=il, check_binaries=check_binaries)

    if ri:
        for ri_directory_to_check in glob.glob('{}{}RI_\d+$'.format(directory_to_check, os.path.sep)):
            _check_each_inputs_directory(ri_directory_to_check, il=True, check_binaries=check_binaries)


def _check_each_inputs_directory(directory_to_check, il=False, check_binaries=True):
    """
    Detailed check of a specific directory
    """

    if il:
        input_files = (f['name'] for f in INPUT_FILES.values() if f['type'] != 'optional')
    else:
        input_files = (f['name'] for f in INPUT_FILES.values() if f['type'] not in ['optional', 'il'])

    for input_file in input_files:
        file_path = os.path.join(directory_to_check, input_file + ".csv")
        if not os.path.exists(file_path):
            raise OasisException("Failed to find {}".format(file_path))

        if check_binaries:
            file_path = os.path.join(directory_to_check, input_file + ".bin")
            if os.path.exists(file_path):
                raise OasisException("Binary file already exists: {}".format(file_path))


@oasis_log
def csv_to_bin(csv_directory, bin_directory, il=False, ri=False):
    """
    Create the binary files.

    :param csv_directory: the directory containing the CSV files
    :type csv_directory: str

    :param bin_directory: the directory to write the binary files
    :type bin_directory: str

    :param il: whether to create the binaries required for insured loss calculations
    :type il: bool

    :param ri: whether to create the binaries required for reinsurance calculations
    :type ri: bool

    :raises OasisException: If one of the conversions fails
    """
    csvdir = os.path.abspath(csv_directory)
    bindir = os.path.abspath(bin_directory)

    il = il or ri

    _csv_to_bin(csvdir, bindir, il)

    if ri:
        for ri_csvdir in glob.glob('{}{}RI_[0-9]*'.format(csvdir, os.sep)):
            _csv_to_bin(
                ri_csvdir, os.path.join(bindir, os.path.basename(ri_csvdir)), il=True)


def _csv_to_bin(csv_directory, bin_directory, il=False):
    """
    Create a set of binary files for the files specified in INPUT_FILES (under files.py)
    """
    if not os.path.exists(bin_directory):
        os.mkdir(bin_directory)

    if il:
        input_files = INPUT_FILES.values()
    else:
        input_files = (f for f in INPUT_FILES.values() if f['type'] != 'il')

    for input_file in input_files:

        input_file_path = os.path.join(csv_directory, '{}.csv'.format(input_file['name']))

        # Check if the source file exists
        if os.path.exists(input_file_path):
            csvfile_to_bin(input_file['name'], csv_directory, bin_directory, "")

    # Now deal with return periods, occurrences and periods

@oasis_log
def csv_to_bin_model_inputs(csv_directory, bin_directory, file_list, analysis_settings):
    """Make sure all input .bin files are up-to-date in the input_folder"""

    if not file_list:
        print("input conversion: nothing to be done")
        return

    # # Check for conversion tools
    # check_conversion_tools(input_files)

    # Suffix for the file from the settings file
    model_settings = analysis_settings.get('model_settings', {})
    setting_val = model_settings.get('event_set')
    setting_val = str(setting_val).replace(' ', '_').lower()
    if setting_val:
        setting_val = "_{}".format(setting_val)

    # Convert input files
    for f in file_list:

        # Check if thee is a different file name
        if setting_val and (f == "events" or f == "occurrence" or f == "periods"):
            csv_file = os.path.join(csv_directory, '{}{}.csv'.format(f, setting_val))
        else:
            csv_file = os.path.join(csv_directory, f + ".csv")

        # Check if csv file exists
        if not os.path.exists(csv_file):
            raise OasisException("Necessary input file {} does not exist".format(csv_file))

        # Set up command line options
        if f == "occurrence":

            # For occurrence file we need the number of periods and the format

            # Read the occurrence file
            occurrence = pd.read_csv(csv_file)

            # Get the number of periods in the occurrence file. This can be overridden
            # by analysis settings
            n_periods = 1 + occurrence['period_no'].max() - occurrence['period_no'].min()
            if 'number_of_periods' in analysis_settings['model_settings']:
                user_n_periods = analysis_settings['model_settings']['number_of_periods']

                # Only overwrite if user defined number is larger than the number in file
                if user_n_periods > n_periods:
                    n_periods = user_n_periods
                elif user_n_periods < n_periods:
                    warnings.warn(("User defined number of periods ({:d}) less than " +
                                   "number in the occurrence file ({:d}). " +
                                   "Using {:d}").format(user_n_periods, n_periods,
                                                      n_periods))

            options = ("-P%i" % n_periods)

            # Check the occurrence file type
            if "occ_date_id" in occurrence.columns:
                options = options + ' -D'

        else:
            # All others, no options are needed
            options = ""

        csvfile_to_bin(f, csv_file, bin_directory, options=options)


@oasis_log
def check_binary_tar_file(tar_file_path, check_il=False):
    """
    Checks that all required files are present

    :param tar_file_path: Path to the tar file to check
    :type tar_file_path: str

    :param check_il: Flag whether to check insured loss files
    :type check_il: bool

    :raises OasisException: If a required file is missing

    :return: True if all required files are present, False otherwise
    :rtype: bool
    """
    expected_members = ('{}.bin'.format(f['name']) for f in GUL_INPUT_FILES.values())

    if check_il:
        expected_members = chain(expected_members, ('{}.bin'.format(f['name']) for f in IL_INPUT_FILES.values()))

    with tarfile.open(tar_file_path) as tar:
        for member in expected_members:
            try:
                tar.getmember(member)
            except KeyError:
                raise OasisException('{} is missing from the tar file {}.'.format(member, tar_file_path))

    return True


@oasis_log
def create_binary_tar_file(directory):
    """
    Package the binaries in a gzipped tar.

    :param directory: Path containing the binaries
    :type directory: str
    """
    with tarfile.open(os.path.join(directory, TAR_FILE), "w:gz") as tar:
        for f in glob.glob('{}*{}*.bin'.format(directory, os.sep)):
            logging.info("Adding {} {}".format(f, os.path.relpath(f, directory)))
            relpath = os.path.relpath(f, directory)
            tar.add(f, arcname=relpath)

        for f in glob.glob('{}*{}*{}*.bin'.format(directory, os.sep, os.sep)):
            relpath = os.path.relpath(f, directory)
            tar.add(f, arcname=relpath)


@oasis_log
def check_conversion_tools(il=False):
    """
    Check that the conversion tools are available

    :param il: Flag whether to check insured loss tools
    :type il: bool

    :return: True if all required tools are present, False otherwise
    :rtype: bool
    """
    if il:
        input_files = INPUT_FILES.values()
    else:
        input_files = (f for f in INPUT_FILES.values() if f['type'] != 'il')

    for input_file in input_files:
        tool = input_file['conversion_tool']
        if shutilwhich.which(tool) is None:
            error_message = "Failed to find conversion tool: {}".format(tool)
            logging.error(error_message)
            raise OasisException(error_message)

    return True


@oasis_log
def cleanup_bin_directory(directory):
    """
    Clean the tar and binary files.
    """
    for file in chain([TAR_FILE], (f + '.bin' for f in INPUT_FILES.keys())):
        file_path = os.path.join(directory, file)
        if os.path.exists(file_path):
            os.remove(file_path)
