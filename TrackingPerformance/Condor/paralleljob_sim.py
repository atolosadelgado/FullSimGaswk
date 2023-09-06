#!/usr/bin/env python

import os
import argparse
import subprocess
import time;
ts = time.time()

thetaList_         = ["10", "20", "30", "40", "50", "60", "70", "80", "89"]
momentumList_      = [ "1", "2", "5", "10", "20", "50", "100", "200"]
particleList_      = [ "mu", "e", "pi"]
DetectorModelList_ = [ "FCCee_o3_v01"]
Nevts_             = "10"

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

# do not expose path to your home
from pathlib import Path
myhome = str(Path.home())
SteeringFile = myhome + "/work/CLICPerformance/fcceeConfig/fcc_steer.py"
run_sim_path = myhome + "/work/GRID/FullSimGaswk/TrackingPerformance/Condor/run_ddsim.py"

if not Path(SteeringFile).is_file():
    raise ModuleNotFoundError(SteeringFile)

if not Path(run_sim_path).is_file():
    raise ModuleNotFoundError(run_sim_path)

workingDir='/tmp'
EosDir = f"/tmp/Output/TrackingPerformance/LCIO/{DetectorModelList_[0]}/SIM/Test_splitting"

#f"/eos/user/g/gasadows/Output/TrackingPerformance/LCIO/{DetectorModelList_[0]}/SIM/Test_splitting"

# Create EosDir if it does not exist
if not os.path.exists(EosDir):
    os.makedirs(EosDir)

# Function to split the tasks into chunks of 200
def chunk_tasks(tasks_list, chunk_size=1):
    for i in range(0, len(tasks_list), chunk_size):
        yield tasks_list[i:i + chunk_size]

# Create the Condor submit script and submit jobs in chunks
condor_file_template = '''
executable = bash_script.sh
arguments = $(ClusterId) $(ProcId)
output = output.$(ClusterId).$(ProcId).out
error = error.$(ClusterId).$(ProcId).err
log = log.$(ClusterId).log
queue {}
'''
#+JobFlavour = "microcentury"   # 1 hour
#+JobFlavour = "longlunch"     # 2 hours
#+JobFlavour = "workday"        # 8 hours

# Calculate the total number of tasks
total_tasks = len(DetectorModelList_) * len(particleList_) * len(thetaList_) * len(momentumList_)

# Split tasks into chunks of 200
task_chunks = list(chunk_tasks(range(total_tasks)))

import numpy as np
rng = np.random.default_rng()

# Loop over task chunks and create a Condor job submission for each chunk
counter = 0

directory_jobs = f"CondorJobs_X"
os.system(f"mkdir -p {directory_jobs}")
for i, chunk in enumerate(task_chunks):
    counter +=1

    bash_file = os.path.join(directory_jobs, f"mysim{counter}.sh")
    with open(bash_file, "w") as file:
        file.write("#!/bin/bash \n")
        file.write("source "+ setup + "\n")

        # Loop over tasks in the current chunk
        for task_id in chunk:
            dect, part, theta, momentum = [DetectorModelList_[task_id // (len(particleList_) * len(thetaList_) * len(momentumList_))],
                                           particleList_[(task_id // (len(thetaList_) * len(momentumList_))) % len(particleList_)],
                                           thetaList_[(task_id // len(momentumList_)) % len(thetaList_)],
                                           momentumList_[task_id % len(momentumList_)]]

            output_file = "SIM_" + dect + "_" + part + "_" + theta + "_deg_" + momentum + "_GeV_" + Nevts_ + "_evts.slcio"

            # time.sleep(1)
            seed = rng.integers(pow(2,63)) #str(time.time() % 1000)
            ddsim_args =""
            ddsim_args+=f" --compactFile ${{LCGEO}}/FCCee/compact/{dect}/{dect}.xml \ \n\t\t"
            ddsim_args+=f" --outputFile  {workingDir}/{output_file} \ \n\t\t"
            ddsim_args+=f" --steeringFile  {SteeringFile} \ \n\t\t"
            ddsim_args+=f" --random.seed  {seed} \ \n\t\t"
            ddsim_args+=f" --numberOfEvents {Nevts_} \ \n\t\t"
            ddsim_args+=f" --enableGun \ \n\t\t"
            ddsim_args+=f" --gun.particle {part} \ \n\t\t"
            ddsim_args+=f" --gun.energy {momentum}*GeV \ \n\t\t"
            ddsim_args+=f" --gun.distribution uniform \ \n\t\t"
            ddsim_args+=f" --gun.thetaMin {theta}*deg \ \n\t\t"
            ddsim_args+=f" --gun.thetaMax {theta}*deg \ \n\t\t"
            ddsim_args+=f" --crossingAngleBoost 0 \n"
            command = "ddsim " + ddsim_args

            file.write(command)
            file.write( f"cp {workingDir}/{output_file} {EosDir}\n")
            file.write( f"date > {EosDir}/{output_file}.kk\n")

    os.chmod(bash_file, 0o755)

    # Create the Condor submit script for this chunk
    condor_file = os.path.join(directory_jobs, "condor_script.sub")
    with open(condor_file, "w") as file2:
        file2.write(condor_file_template.format(len(chunk)))

    # Submit the Condor job for this chunk
    #os.system(f"cd {directory_jobs}; condor_submit condor_script.sub")


