import subprocess

command = 'Rscript'
scriptPath = '/Users/mehadi/work/d2health/DQe-c-v2/mhTest/run.R'

subprocess.check_output([command, scriptPath], universal_newlines=True)