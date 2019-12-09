import csv
import io
import json
import logging
import os
import warnings

from collections import defaultdict

from ..utils.exceptions import OasisException
from ..utils.log import oasis_log
from .files import GENERAL_SETTINGS_FILE, GUL_SUMMARIES_FILE, IL_SUMMARIES_FILE, MODEL_SETTINGS_FILE


def _get_summaries(summary_file):
    """
    Get a list representation of a summary file.
    """
    summaries_dict = defaultdict(lambda: {'leccalc': {}})

    with io.open(summary_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            id = int(row[0])

            if row[1].startswith('leccalc'):
                summaries_dict[id]['leccalc'][row[1]] = row[2].lower() == 'true'
            else:
                summaries_dict[id][row[1]] = row[2].lower() == 'true'

    summaries = list()
    for id in sorted(summaries_dict):
        summaries_dict[id]['id'] = id
        summaries.append(summaries_dict[id])

    return summaries


@oasis_log
def create_analysis_settings_json(directory):
    """
    Generate an analysis settings JSON from a set of
    CSV files in a specified directory.
    Args:
        ``directory`` (string): the directory containing the CSV files.
    Returns:
        The analysis settings JSON.
    """
    if not os.path.exists(directory):
        error_message = "Directory does not exist: {}".format(directory)
        logging.getLogger().error(error_message)
        raise OasisException(error_message)

    general_settings_file = os.path.join(directory, GENERAL_SETTINGS_FILE)
    model_settings_file = os.path.join(directory, MODEL_SETTINGS_FILE)
    gul_summaries_file = os.path.join(directory, GUL_SUMMARIES_FILE)
    il_summaries_file = os.path.join(directory, IL_SUMMARIES_FILE)

    for file in [general_settings_file, model_settings_file, gul_summaries_file, il_summaries_file]:
        if not os.path.exists(file):
            error_message = "File does not exist: {}".format(directory)
            logging.getLogger().error(error_message)
            raise OasisException(error_message)

    general_settings = dict()
    with io.open(general_settings_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            general_settings[row[0]] = eval("{}('{}')".format(row[2], row[1]))

    model_settings = dict()
    with io.open(model_settings_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            model_settings[row[0]] = eval("{}('{}')".format(row[2], row[1]))

    gul_summaries = _get_summaries(gul_summaries_file)
    il_summaries = _get_summaries(il_summaries_file)

    analysis_settings = general_settings
    analysis_settings['model_settings'] = model_settings
    analysis_settings['gul_summaries'] = gul_summaries
    analysis_settings['il_summaries'] = il_summaries
    output_json = json.dumps(analysis_settings)
    logging.getLogger().info("Analysis settings json: {}".format(output_json))

    return output_json


def read_analysis_settings(analysis_settings_fp, il_files_exist=True,
                           ri_files_exist=True):
    """Read the analysis settings file

    Arguments:
        analysis_settings_fp: (str) filename for the analysis settings json

        il_files_exist: (bool) flag in case we know that necessary insured loss files
        do not exist. Default True

        ri_files_exist: (bool) flag in case we know that

    Returns:
        analysis_settings: (dict) a dict representation of the input json file
    """

    # Load analysis_settings file
    try:
        # Load as a json
        with io.open(analysis_settings_fp, 'r', encoding='utf-8') as f:
            analysis_settings = json.load(f)

        # Extract the analysis_settings part within the json
        if analysis_settings.get('analysis_settings'):
            analysis_settings = analysis_settings['analysis_settings']

    except (IOError, TypeError, ValueError):
        raise OasisException('Invalid analysis settings file or file path: {}.'.format(
            analysis_settings_fp))

    # Reset il_output if the files are not there
    if not il_files_exist or 'il_output' not in analysis_settings:
        # No insured loss output
        analysis_settings['il_output'] = False
        analysis_settings['il_summaries'] = []

    # Same for ri_output
    if not ri_files_exist or 'ri_output' not in analysis_settings:
        # No reinsured loss output
        analysis_settings['ri_output'] = False
        analysis_settings['ri_summaries'] = []

    # If we want ri_output, we will need il_output, which needs il_files
    if analysis_settings['ri_output'] and not analysis_settings['il_output']:
        if not il_files_exist:
            warnings.warn("ri_output selected, but il files not found")
            analysis_settings['ri_output'] = False
            analysis_settings['ri_summaries'] = []
        else:
            analysis_settings['il_output'] = True

    # guard - Check if at least one output type is selected
    if not any([
        analysis_settings['gul_output'] if 'gul_output' in analysis_settings else False,
        analysis_settings['il_output'] if 'il_output' in analysis_settings else False,
        analysis_settings['ri_output'] if 'ri_output' in analysis_settings else False,
    ]):
        raise OasisException(
            'No valid output settings in: {}'.format(analysis_settings_fp))

    return analysis_settings
