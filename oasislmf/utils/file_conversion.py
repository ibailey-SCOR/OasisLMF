# -*- coding: utf-8 -*-
"""
    Python utilities used for converting csv files to binary format
"""

import os
import warnings
import shutil
import subprocess
import pandas as pd

from .exceptions import OasisException

# List of all the oasis files and relevant conversion files
CONVERSION_TOOLS = {
    'items': 'itemtobin',
    'coverages': 'coveragetobin',
    'gulsummaryxref': 'gulsummaryxreftobin',
    'events': 'evetobin',
    'returnperiods': 'returnperiodtobin',
    'occurrence': 'occurrencetobin',
    'fm_policytc': 'fmpolicytctobin',
    'fm_profile': 'fmprofiletobin',
    'fm_programme': 'fmprogrammetobin',
    'fm_xref': 'fmxreftobin',
    'fmsummaryxref': 'fmsummaryxreftobin',
    'footprint': 'footprinttobin',
    'vulnerability': 'vulnerabilitytobin',
    'damage_bin_dict': 'damagebintobin',
    'random': 'randtobin'}


def check_conversion_tools(filenamelist=None, is_quiet=False):
    """
    Check that the conversion tools are available
    """

    if filenamelist is None:
        filenamelist = CONVERSION_TOOLS.keys()

    is_ok = True
    if not is_quiet:
        print("Checking conversion tools")

    for input_file in filenamelist:
        tool = CONVERSION_TOOLS[input_file]

        if shutil.which(tool) is None:
            raise OasisException("Conversion tool %s: NOT FOUND" % tool)
            # error_message = "Failed to find conversion tool: {}".format(tool)
            # logging.error(error_message)
            # print(error_message)
            # is_ok = False
            # #raise OasisException(error_message)
        else:
            if not is_quiet:
                print("%s: OK" % tool)

    return is_ok


def get_required_static_files(analysis_settings):
    """Based on config options, return a list of static data files that
    will be required.

    """

    # Start with list of files that are always required
    static_files = ['footprint', 'vulnerability', 'damage_bin_dict']

    # Check if random is required
    if 'model_settings' in analysis_settings:
        if (('use_random_number_file'in analysis_settings['model_settings']) and
                (analysis_settings['model_settings']['use_random_number_file'])):
         static_files.append('random')

    return static_files


def get_required_model_inputs(analysis_settings):
    """Based on config options, return a list of model input data files (not exposure related) that
    will be required.

    """

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
        input_files.append('{}{}'.format('occurrence', setting_val))

    if is_rp:
        input_files.append('returnperiods')

    # TODO: periods

    return input_files


def check_new_bin_needed(filename, csv_directory, bin_directory=None):
    """Return true if a conversion should and can be done

    csv file exists and
    EITHER .bin file doesn't exist or is older

    """

    # Default bin directory is the same as the csv folder
    if bin_directory is None:
        bin_directory = csv_directory

    # Define folder names
    csvfile = os.path.join(csv_directory, filename + '.csv')
    binfile = os.path.join(bin_directory, filename + '.bin')

    # Special case for footprint which also requires idx file
    idxfile = os.path.join(bin_directory, filename + '.idx')

    # Check if a newer bin file is needed
    is_needed = False
    if os.path.exists(csvfile):
        if not os.path.exists(binfile):
            # Only the csv exists so we must generate the bin
            is_needed = True

        elif filename == "footprint" and not os.path.exists(idxfile):
            # footprint.bin exists but not the footprint.idx... we must generate both
            is_needed = True

        else:
            # Check if the csv file is newer. If not, is_needed is false
            if filename == "footprint":
                is_needed = ((os.path.getmtime(csvfile) > os.path.getmtime(binfile)) or
                             (os.path.getmtime(csvfile) > os.path.getmtime(idxfile)))
            else:
                is_needed = os.path.getmtime(csvfile) > os.path.getmtime(binfile)
    else:
        # Only the bin exists, so we must rely on it
        if not os.path.exists(binfile):
            # Neither file exists. Raise a warning
            warnings.warn("Neither %s.csv or %s.bin exist" %
                          (filename, filename))

        if filename == "footprint" and not os.path.exists(idxfile):
            warnings.warn("Neither %s.csv or %s.idx exist" %
                          (filename, filename))

    return is_needed


def get_necessary_conversions(fileprefixes, csv_folder, bin_folder=None):
    """Return a list on file prefixes for which a csv to bin conversion is
    necessary. Each is evaluated based on the function
    'new_bin_is_needed'.

    """

    newfileprefixes = []

    for f in fileprefixes:
        if check_new_bin_needed(f, csv_folder, bin_folder):
            newfileprefixes.append(f)

    return newfileprefixes


def clean_bins(directory, filelist=None, check_csv=True, csv_folder=None):
    """
    Clean the specfied binary files from the associated folder.
    """

    if not filelist:
        filelist = CONVERSION_TOOLS.keys()

    if not csv_folder:
        csv_folder = directory

    # Loop through each file
    for f in filelist:

        file_path = os.path.join(directory, f)

        # Check first if file exists
        if not os.path.exists(file_path + ".bin"):
            continue

        # Check if csv file exists
        if check_csv and not os.path.exists(os.path.join(csv_folder, f + ".csv")):
            warnings.warn("{}: csv file not found so bin file not deleted".format(f))
            continue

        print("Removing %s" % file_path + ".bin")
        os.remove(file_path + ".bin")

        if f == "footprint" and os.path.exists(file_path + ".idx"):
            print("Removing %s" % file_path + ".idx")
            os.remove(file_path + ".idx")


def footprint_csv_to_bin(input_file_path, directory, options):
    """Convert the footprint file to binary
    Options
    -i max intensity bins
    -n No intensity uncertainty
    -s skip header
    """

    cmd_str = "{} {} < {}".format(CONVERSION_TOOLS['footprint'],
                                  options,
                                  input_file_path)

    print(cmd_str)

    # Run the command
    try:
        # subprocess.check_call(cmd_str, stderr=subprocess.STDOUT, shell=True)
        proc = subprocess.Popen(cmd_str,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                universal_newlines=True)
        print(proc.stdout.read())
        # proc.communicate()
    except subprocess.CalledProcessError as e:
        raise Exception(e)

    def movefile(src, dst):
        """Move the files, including overwrite"""
        if os.path.exists(dst) and not os.path.samefile(src, dst):
            os.remove(dst)
        shutil.move(src, dst)

    movefile("footprint.bin", os.path.join(directory, "footprint.bin"))
    movefile("footprint.idx", os.path.join(directory, "footprint.idx"))


def csvfile_to_bin(filename, input_file_path, bin_folder=None, options=""):
    """Convert a csv to bin in the same folder"""

    if bin_folder is None:
        bin_folder = os.path.dirname(input_file_path)

    # Do nothing if file isn't there
    if not os.path.exists(input_file_path):
        raise Exception("csv file [%s] doesn't exist" % input_file_path)

    # Special case for footprint
    if filename == "footprint":
        if not options:
            raise Exception("Options are required for footprint conversion")
        footprint_csv_to_bin(input_file_path, bin_folder, options)
        return

    output_file_path = os.path.join(bin_folder, filename + ".bin")

    # Get the conversion command string
    cmd_str = "{} {} < {} > {}".format(CONVERSION_TOOLS[filename],
                                       options,
                                       input_file_path,
                                       output_file_path)

    # print(cmd_str)

    # Run the command
    try:
        # subprocess.check_call(cmd_str, stderr=subprocess.STDOUT,
        #                             shell=True)
        proc = subprocess.Popen(cmd_str, shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                universal_newlines=True)
        stdout_text = proc.stdout.read().strip()
        if stdout_text:
            print(stdout_text)
        # proc.communicate()
    except subprocess.CalledProcessError as e:
        raise Exception(e)

    return


def update_static_bin_files(static_folder, static_files, is_intensity_uncertainty=False, is_force=False):
    """Make sure all static .bin files are up-to-date in the static_folder"""
    if is_force:
        clean_bins(static_folder)

    # Get which files need updating
    static_files = get_necessary_conversions(static_files,
                                             static_folder)

    if not static_files:
        print("static conversion: nothing to be done")
        return

    # Check for conversion tools
    check_conversion_tools(static_files)

    # Convert static files
    for f in static_files:

        # Set up command line options
        if f == "vulnerability":
            damagebins = pd.read_csv(os.path.join(static_folder,
                                                  "damage_bin_dict.csv"))

            # TODO: needs to be more robust if header is missing or if not called bin_index
            maxdamagebin = damagebins.bin_index.max()

            # Specify the number of damage bins
            options = ("-d%i" % maxdamagebin)

        elif f == "footprint":
            # For the footprint file we need the number of intensity bins

            # TODO: needs to be more robust if intensity_bin_dict is not there
            intensbins = pd.read_csv(os.path.join(static_folder,
                                                  "intensity_bin_dict.csv"))
            maxintensbin = intensbins.intensity_bin_index.max()

            # Specify the numebr of intensity bins and flag if no uncertainty
            options = ("-i%i" % maxintensbin)
            if not is_intensity_uncertainty:
                options = options + " -n"
        else:
            # All others, no options are needed
            options = ""

        csvfile_to_bin(f, static_folder, options=options)

    return
