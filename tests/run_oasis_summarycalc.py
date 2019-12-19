"""Python running of oasis commands"""

import os
import subprocess
from subprocess import Popen

def main():
    """"Main script"""

    current_folder = os.path.abspath(os.getcwd())
    working_folder = "ktools_analysis"
    iproc = 1
    nproc = 4
    n_samples = 10

    # Change folder
    os.chdir(working_folder)
    print(os.getcwd())

    # Generate the stream of event ids
    eve_process = Popen(['eve', str(iproc), str(nproc)], stdout=subprocess.PIPE)

    # out, err = eve_process.communicate()

    # Get the model for affected events/items
    getmodel_process = Popen('getmodel', stdin=eve_process.stdout, stdout=subprocess.PIPE)

    eve_process.stdout.close()  # enable write error in dd if ssh dies

    # out, err = getmodel_process.communicate()

    # Create the ground up loss samples
    gulcalc_process = Popen(['gulcalc', '-r',  "-L{:f}".format(0.0),
                             "-S{:d}".format(n_samples),
                             '-c', '-'], stdin=getmodel_process.stdout,
                             stdout=subprocess.PIPE)

    getmodel_process.stdout.close()


    # Aggregate results to the summarycalc level
    sumcalc_process = Popen(["summarycalc", "-g", "-1", "-"],
                            stdin=gulcalc_process.stdout, stdout=subprocess.PIPE)

    gulcalc_process.stdout.close()

    # Convert to a text file
    
    # csv_process = Popen("summarycalctocsv",
    #                                stdin=sumcalc_process.stdout,
    #                                stdout=subprocess.PIPE)
    #
    # sumcalc_process.stdout.close()
    #
    # output = csv_process.communicate()[0]

    with open("tmp.csv", 'w') as file:
        csv_process = Popen("summarycalctocsv", stdin=sumcalc_process.stdout, stdout=file)
        sumcalc_process.stdout.close()
        csv_process.communicate()

    # Revert back to original folder
    os.chdir(current_folder)

if __name__ == '__main__':
    main()
