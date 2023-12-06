#!/usr/bin/env python3
"""
This script generates 3 files:
1. condor job descriptor
2. bash script
3. text file with arguments

The condor file will spawn as many instances as rows in the arguments text file.
Each instance will execute the bash script, which basically wraps ddsim
The generated files are automatically copied to EOS

Generate files: python3 condorJobs_sim_new.py
Launch Jobs: cd Condor_jobs && condor_submit condor_script.sub
"""


import os
import random;

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Define variable list for the simulation
thetaList_         = [ "45" ]#"10", "20", "30", "40", "50", "60", "70", "80", "89"]
energyList_        = [ "1"]#, "2"]#, "5", "10", "20", "50", "100", "200"]
particleList_      = [ "mu-"] #, "e-", "pi-"]
DetectorModelList_ = [ "CLD_o2_v05"]#,"CLD_o3_v01" ] #"FCCee_o2_v02"]CLD_o3_v01
Nevts_             = "10"

# Create all possible combinations
import itertools
list_of_combined_variables = itertools.product(thetaList_, energyList_, particleList_, DetectorModelList_)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# source key4hep stable
# setup = "/cvmfs/sw.hsf.org/key4hep/setup.sh"
# source key4hep nightlies
setup = "/cvmfs/sw-nightlies.hsf.org/key4hep/setup.sh"
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Create and check paths

# do not expose path to your home
from pathlib import Path
myhome = str(Path.home())
SteeringFilePath = myhome + "/Public/ddsim_steering_CLD.py"

# Condor needs fullpath in order to copy the file
# The file will be placed in the same dir as the bash executable
# so we need also the base name
SteeringFileName = os.path.basename(SteeringFilePath)
# check if files exist
if not Path(SteeringFilePath).is_file():
    raise ModuleNotFoundError(SteeringFilePath)

# myhome[13:] removes the '/afs/cern.ch/' at the begining of the AFS home path
EosDir = f"/eos/{myhome[13:]}/condor/$(ClusterId)/$(ProcId)"
AfsDir = myhome + '/Public'
OutputDir = EosDir

# Work in /tmp, it is faster in local
workingDir='.'

# the job descriptor neither the executable must not be stored in EOS
# https://batchdocs.web.cern.ch/troubleshooting/eos.html#no-eos-submission-allowed
local_dir=str(Path.cwd())
job_dir=f"{local_dir}/Condor_jobs"

# Do not overwrite previous work!
if Path(job_dir).is_dir():
    raise ModuleNotFoundError(f"Well, actually {job_dir} was found and it should not be there...")

os.system(f"mkdir {job_dir}")

# Create the list of arguments, that will passed to a bash script
# and finally the generic condor file that launch the jobs
arg_file_name   =f"{job_dir}/arg_file.txt"
bash_file_name  =f"{job_dir}/bash_script.sh"
condor_file_name=f"{job_dir}/condor_script.sub"

k4geoDir = os.environ.get('K4GEO')
create_local_k4geo = ''
if not Path(k4geoDir).is_dir():
    raise ModuleNotFoundError('k4geo path not found')

# To compile k4geo, uncomment the following lines
# # It seems the batch nodes can not see the local installation in my personal AFS
# # so the solution is to download, compile and install locally the repo for each job
# create_local_k4geo = '''
# git clone -b CLD_with_ARC https://github.com/atolosadelgado/k4geo.git
# cd k4geo/
# cmake -B build -S . -D CMAKE_INSTALL_PREFIX=install
# cmake --build build -j 6 -- install
# export LD_LIBRARY_PATH=$PWD/install/lib:$LD_LIBRARY_PATH
# cd
# '''
# k4geoDir = "./k4geo"

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# This file corresponds to a table, each row corresponds to all the arguments needed
# by the bash script that actually will execute each job
with open(arg_file_name,"w") as arg_file:
    for theta, energy, particle, detectorModel in list_of_combined_variables:
        output_file = f"SIM_{detectorModel}"
        output_file+= f"_{particle}"
        output_file+= f"_{theta}_deg"
        output_file+= f"_{energy}_GeV"
        output_file+= f"_{Nevts_}"
        output_file+= f"_evts.root"

        mv_output_file_args =""
        mv_output_file_args+=f"{workingDir}/{output_file}\t"
        mv_output_file_args+=f"{OutputDir}/{output_file}\t"

        seed = 1 #random.getrandbits(64)
        ddsim_args =""
        ddsim_args+=f" --compactFile {k4geoDir}/FCCee/CLD/compact/{detectorModel}/{detectorModel}.xml \t"
        ddsim_args+=f" --outputFile  {output_file} \t"
        # when transferring input file, the file will appear in the sandbox
        # here we use the local path (base name)
        ddsim_args+=f" --steeringFile  {SteeringFileName} \t"
        ddsim_args+=f" --random.seed  {seed} \t"
        ddsim_args+=f" --numberOfEvents {Nevts_} \t"
        ddsim_args+=f" --enableGun \t"
        ddsim_args+=f" --gun.particle {particle} \t"
        ddsim_args+=f" --gun.energy {energy}*GeV \t"
        ddsim_args+=f" --gun.distribution uniform \t"
        ddsim_args+=f" --gun.thetaMin {theta}*deg \t"
        ddsim_args+=f" --gun.thetaMax {theta}*deg \t"
        ddsim_args+=f" --random.enableEventSeed "
        arg_file.write(f"{mv_output_file_args}{ddsim_args}\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# This bash script takes ddsim arguments, create k4geo locally and then launch ddsim
bash_template = f'''
#!/bin/bash
source {setup}
{create_local_k4geo}
ArgList=( "$@" )
outputOriginal=${{ArgList[0]}}
outputFinal=${{ArgList[1]}}
ddsimArgs=${{ArgList[@]:2}}
echo "ddsimArgs: " $ddsimArgs
echo "ddsim starts simulation..."
date
ddsim $ddsimArgs
xrdcp $outputOriginal $outputFinal
echo "ddsim starts simulation... Done!"
date
date > $outputFinal.date
'''
with open(bash_file_name, "w") as sim_script:
    sim_script.write(bash_template)
os.chmod(bash_file_name, 0o755)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Now lets create the condor job descriptor
# when transferring input file, the file will appear in the sandbox
# here we use the full path
# These options were removed after inestabilities of EOS were detected
# output_destination = root://eosuser.cern.ch/{EosDir}
# MY.XRDCP_CREATE_DIR = True
# Job flavours at CERN:
#   espresso     = 20 minutes
#   microcentury = 1 hour
#   longlunch    = 2 hours
#   workday      = 8 hours
#   tomorrow     = 1 day
#   testmatch    = 3 days
#   nextweek     = 1 week


condor_file_template=f'''
executable = {bash_file_name}
output = output.$(ClusterId).$(ProcId).out
error = error.$(ClusterId).$(ProcId).err
log = log.$(ClusterId).log
should_transfer_files = YES
transfer_input_files = {SteeringFilePath}
transfer_output_files = ""
+JobFlavour = "espresso"
queue arguments from {arg_file_name}
'''
with open(condor_file_name, "w") as condor_file:
    condor_file.write(condor_file_template)
