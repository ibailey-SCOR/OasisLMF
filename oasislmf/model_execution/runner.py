import logging
import multiprocessing
import os
import shutil

import subprocess

from ..utils.exceptions import OasisException
from ..utils.log import oasis_log
from .bash import genbash
from ..utils.defaults import (
    KTOOLS_ALLOC_GUL_DEFAULT,
    KTOOLS_ALLOC_IL_DEFAULT,
    KTOOLS_ALLOC_RI_DEFAULT,
)


@oasis_log()
def run(
    analysis_settings,
    number_of_processes=-1,
    num_reinsurance_iterations=0,
    set_alloc_rule_gul=KTOOLS_ALLOC_GUL_DEFAULT,
    set_alloc_rule_il=KTOOLS_ALLOC_IL_DEFAULT,
    set_alloc_rule_ri=KTOOLS_ALLOC_RI_DEFAULT,
    fifo_tmp_dir=True,
    stderr_guard=True,
    run_debug=False,
    filename='run_ktools.sh'
):
    if number_of_processes == -1:
        number_of_processes = multiprocessing.cpu_count()

    custom_gulcalc_cmd = "{}_{}_gulcalc".format(
        analysis_settings.get('module_supplier_id'),
        analysis_settings.get('model_version_id'))

    # Check for custom gulcalc
    if shutil.which(custom_gulcalc_cmd):

        def custom_get_getmodel_cmd(
            number_of_samples,
            gul_threshold,
            use_random_number_file,
            coverage_output,
            item_output,
            process_id,
            max_process_id,
            gul_alloc_rule,
            **kwargs
        ):

            cmd = "{} -e {} {} -a {} -p {}".format(
                custom_gulcalc_cmd,
                process_id,
                max_process_id,
                os.path.abspath("analysis_settings.json"),
                "input")
            if coverage_output != '' and not gul_alloc_rule:
                cmd = '{} -c {}'.format(cmd, coverage_output)
            if item_output != '':
                cmd = '{} -i {}'.format(cmd, item_output)

            return cmd

        genbash(
            number_of_processes,
            analysis_settings,
            num_reinsurance_iterations=num_reinsurance_iterations,
            fifo_tmp_dir=fifo_tmp_dir,
            gul_alloc_rule=set_alloc_rule_gul,
            il_alloc_rule=set_alloc_rule_il,
            ri_alloc_rule=set_alloc_rule_ri,
            stderr_guard=stderr_guard,
            bash_trace=run_debug,
            filename=filename,
            _get_getmodel_cmd=custom_get_getmodel_cmd,
        )
    else:
        genbash(
            number_of_processes,
            analysis_settings,
            num_reinsurance_iterations=num_reinsurance_iterations,
            fifo_tmp_dir=fifo_tmp_dir,
            gul_alloc_rule=set_alloc_rule_gul,
            il_alloc_rule=set_alloc_rule_il,
            ri_alloc_rule=set_alloc_rule_ri,
            stderr_guard=stderr_guard,
            bash_trace=run_debug,
            filename=filename
        )

    bash_trace = subprocess.check_output(['bash', filename])
    logging.info(bash_trace.decode('utf-8'))
