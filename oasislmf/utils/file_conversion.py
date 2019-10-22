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


def check_conversion_tools(filenamelist=None):
    """
    Check that the conversion tools are available
    """

    if filenamelist is None:
        filenamelist = CONVERSION_TOOLS.keys()

    for input_file in filenamelist:
        tool = CONVERSION_TOOLS[input_file]

        if shutil.which(tool) is None:
            raise OasisException("Conversion tool %s: NOT FOUND" % tool)
            # error_message = "Failed to find conversion tool: {}".format(tool)
            # logging.error(error_message)
            # print(error_message)
            # is_ok = False
            # #raise OasisException(error_message)


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


def is_new_bin_needed(filename, csv_directory, bin_directory=None):
    """Return true if a conversion should and can be done

    csv file exists and
    EITHER .bin file doesn't exist or is older

    """

    # Default bin directory is the same as the csv folder
    if bin_directory is None:
        bin_directory = csv_directory

    # Define file names
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
                # TODO: Case where bin and idx are different times, but both are newer
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


def footprint_csv_to_bin(input_file_path, directory, options):
    """Convert the footprint file to binary. This is a special case because two files
    are generated... footprint.bin and footprint.idx

    Options for footprinttobin
    -i max intensity bins
    -n No intensity uncertainty
    -s skip header
    """

    cmd_str = "{} {} < {}".format(CONVERSION_TOOLS['footprint'],
                                  options,
                                  input_file_path)

    # print(cmd_str)

    # Run the command
    try:
        # subprocess.check_call(cmd_str, stderr=subprocess.STDOUT, shell=True)
        proc = subprocess.Popen(cmd_str,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                universal_newlines=True)

        stdout_text = proc.stdout.read().strip()
        if stdout_text:
            print("STDOUT:\n{}".format(stdout_text))

    except subprocess.CalledProcessError as e:
        raise Exception(e)

    def movefile(src, dst):
        """Move the files, including overwrite"""
        if os.path.exists(dst) and not os.path.samefile(src, dst):
            os.remove(dst)
        shutil.move(src, dst)

    movefile("footprint.bin", os.path.join(directory, "footprint.bin"))
    movefile("footprint.idx", os.path.join(directory, "footprint.idx"))


def csvfile_to_bin(filename, csv_folder, bin_folder=None, options=""):
    """Convert a csv to bin file"""

    if bin_folder is None:
        bin_folder = csv_folder

    # Do nothing if file isn't there
    input_file_path = os.path.join(csv_folder, filename + ".csv")
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

    # Print a message
    print("\tcsvtobin: {} {}".format(filename, options))

    # Run the command
    try:
        # subprocess.check_call(cmd_str, stderr=subprocess.STDOUT, shell=True)
        proc = subprocess.Popen(cmd_str, shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                universal_newlines=True)
        stdout_text = proc.stdout.read().strip()
        if stdout_text:
            print("STDOUT:\n{}".format(stdout_text))
        # proc.communicate()
    except subprocess.CalledProcessError as e:
        raise Exception(e)

    return


def get_occurrence_csvtobin_options(csvdir, settings):
    """Return the command line options for converting the occurrence file from csv to bin

    Converting occurrence file from csv to bin requires us to know the format and
    the number of total periods.
    """

    format_flag = ""

    # Check if the csv file exists
    occurrence_csv_file = os.path.join(csvdir, 'occurrence.csv')
    if not os.path.exists(occurrence_csv_file):
        warnings.warn("occurrence file doesn't exist. Can't determine format")
        occurrence = None
    else:
        # Read the file
        occurrence = pd.read_csv(occurrence_csv_file)
        if "occ_date_id" in occurrence.columns:
            format_flag = '-D'

    if 'model_settings' in settings and 'number_of_periods' in settings['model_settings']:
        number_of_periods = settings['model_settings']['number_of_periods']
    elif occurrence is not None:
        warnings.warn("Number of periods not specified in settings - using max period_no")
        number_of_periods = 1 + occurrence.period_no.max() - occurrence.period_no.min()
    else:
        number_of_periods = None

    if number_of_periods is not None:
        options = "-P{:d}".format(number_of_periods)

    if format_flag:
        options += " " + format_flag

    return options


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


def get_necessary_conversions(fileprefixes, csv_folder, bin_folder=None):
    """Return a list on file prefixes for which a csv to bin conversion is
    necessary. Each is evaluated based on the function
    'new_bin_is_needed'.

    """

    newfileprefixes = []

    for f in fileprefixes:
        if is_new_bin_needed(f, csv_folder, bin_folder):
            newfileprefixes.append(f)

    return newfileprefixes


def update_static_bin_files(static_folder, static_files, is_intensity_uncertainty=False,
                            is_force=False):
    """Make sure all static .bin files are up-to-date in the static_folder"""

    if not is_force:
        # Get which files need updating
        static_files = get_necessary_conversions(static_files, static_folder)

    if not static_files:
        print("static conversion: nothing to be done")
        return
    else:
        print("Files to be updated: {}".format(static_files))

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

        csv_filename = os.path.join(static_folder, f + ".csv")

        print("\tConverting csv -> bin for {}".format(csv_filename))
        csvfile_to_bin(f, static_folder, static_folder, options=options)

    return


