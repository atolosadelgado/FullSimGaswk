#!/usr/bin/env python

import os
import random;

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Define variable list for the simulation
thetaList_         = ["10", "20", "30", "40", "50", "60", "70", "80", "89"]
energyList_      = [ "1", "2", "5", "10", "20", "50", "100", "200"]
particleList_      = [ "mu", "e", "pi"]
DetectorModelList_ = [ "FCCee_o2_v02" ] #"FCCee_o2_v02"]CLD_o3_v01
Nevts_             = "10"

# Create all possible combinations
import itertools
list_of_combined_variables = itertools.product(thetaList_, energyList_, particleList_, DetectorModelList_)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# setup for Centos7, stable
setup = "/cvmfs/sw.hsf.org/key4hep/setup.sh"
# in case of Alma8/9, use nightlies instead
try:
    import distro
    osname,version,kk = distro.linux_distribution()
    version = float(version)
    if 7 < version :
        setup = '/cvmfs/sw-nightlies.hsf.org/key4hep/setup.sh'
except:
    print("Asuming Centos 7, stable stack")
    print(f"Setup: {setup}")
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Create and check paths

# do not expose path to your home
from pathlib import Path
myhome = str(Path.home())
SteeringFile = myhome + "/work/CLICPerformance/fcceeConfig/fcc_steer.py"
myk4geo_path = myhome + "/work/CLD_with_ARC/k4geo/"

# check if files exist
if not Path(SteeringFile).is_file():
    raise ModuleNotFoundError(SteeringFile)

# myhome[13:] removes the '/afs/cern.ch/' at the begining of the AFS home path
EosDir = f"/eos/{myhome[13:]}/Output/TrackingPerformance/LCIO/{DetectorModelList_[0]}/SIM/Test_splitting"

# Work in /tmp, it is faster!
workingDir='/tmp'

# Create EosDir is it does not exist
if not os.path.exists(EosDir):
    os.makedirs(EosDir)

job_dir=f"{EosDir}/Condor_jobs"
if not Path(job_dir).is_dir():
    os.system(f"mkdir -p {job_dir}")

arg_file_name   =f"{job_dir}/arg_file.txt"
bash_file_name  =f"{job_dir}/bash_script.sh"
condor_file_name=f"{job_dir}/condor_script.sub"

if Path(arg_file_name).is_file():
    raise ModuleNotFoundError(f"Well, actually {arg_file_name} was found and it should not be there...")
if Path(bash_file_name).is_file():
    raise ModuleNotFoundError(f"Well, actually {bash_file_name} was found and it should not be there...")

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
        output_file+= f"_evts.slcio"

        mv_output_file_args =""
        mv_output_file_args+=f"{workingDir}/{output_file}\t"
        mv_output_file_args+=f"{EosDir}/{output_file}\t"

        # time.sleep(1)
        seed = random.getrandbits(64)
        ddsim_args =""
        ddsim_args+=f" --compactFile {myk4geo_path}/FCCee/compact/{detectorModel}/{detectorModel}.xml \t"
        ddsim_args+=f" --outputFile  {workingDir}/{output_file} \t"
        ddsim_args+=f" --steeringFile  {SteeringFile} \t"
        ddsim_args+=f" --random.seed  {seed} \t"
        ddsim_args+=f" --numberOfEvents {Nevts_} \t"
        ddsim_args+=f" --enableGun \t"
        ddsim_args+=f" --gun.particle {particle} \t"
        ddsim_args+=f" --gun.energy {energy}*GeV \t"
        ddsim_args+=f" --gun.distribution uniform \t"
        ddsim_args+=f" --gun.thetaMin {theta}*deg \t"
        ddsim_args+=f" --gun.thetaMax {theta}*deg \t"
        ddsim_args+=f" --crossingAngleBoost 0 "
        arg_file.write(f"{mv_output_file_args}{ddsim_args}\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# This bash script takes as argument an integer, starting by 0
# and then it will read the correponding row from the argument table
# execute the simulation using ddsim in workingDir(/tmp)
# and move the final file to EOS directory
with open(bash_file_name, "w") as sim_script:
        sim_script.write("#!/bin/bash \n")
        sim_script.write("n=$1 \n")
        sim_script.write(f"source {setup}\n")
        sim_script.write(f"export LD_LIBRARY_PATH={myk4geo_path}/install/lib:$LD_LIBRARY_PATH\n")
        # Get line n+1, n will correspond to job number, which starts by zero
        sim_script.write(f"ArgList=( $(head -n $(($n+1)) {arg_file_name}|tail -1) )\n")
        # the first string corresponds to {workingDir}/{output_file}
        sim_script.write(f"outputOriginal=${{ArgList[0]}}\n")

        # the second string corresponds to {EosDir}/{output_file}
        sim_script.write(f"outputFinal=${{ArgList[1]}}\n")

        # the ddsim arguments are all except the first 2 strings
        sim_script.write(f"ddsimArgs=${{ArgList[@]:2}}\n")
        sim_script.write(f"echo 'outputOriginal: ' $outputOriginal\n")
        sim_script.write(f"echo 'outputFinal: ' $outputFinal\n")
        sim_script.write(f"echo 'ddsimArgs: ' $ddsimArgs\n")
        sim_script.write(f"ddsim $ddsimArgs\n")
        sim_script.write(f"mv $outputOriginal $outputFinal\n")
        sim_script.write( f"date > $outputFinal.date\n")

os.chmod(bash_file_name, 0o755)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Now lets create the condor job descriptor
# queue X means how many times the instance will be launched
# if X=5, it means the executable bash_script.sh will be run 5 times
# the variable $(ProcId) will change for each instance, from 0 to X-1
# the variable $(ClusterId) is the same for all instances
condor_file_template = '''
executable = bash_script.sh
arguments = $(ProcId)
output = output.$(ClusterId).$(ProcId).out
error = error.$(ClusterId).$(ProcId).err
log = log.$(ClusterId).log
queue {}
'''
#+JobFlavour = "microcentury"   # 1 hour
#+JobFlavour = "longlunch"     # 2 hours
#+JobFlavour = "workday"        # 8 hours
with open(condor_file_name, "w") as condor_file:
    n=len(DetectorModelList_) * len(particleList_) * len(thetaList_) * len(energyList_)
    condor_file.write(condor_file_template.format(n))
# os.system(f"cd {job_dir}; condor_submit condor_script.sub")


