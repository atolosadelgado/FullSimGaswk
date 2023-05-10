import ROOT
import os
import glob
import numpy as np
import scipy as scipy
import matplotlib.pyplot as plt
from statistics import mean # importing mean()
from scipy.optimize import curve_fit  # importing curve_fit
from scipy.stats import norm

# Get the current working directory
cwd = os.getcwd()

# Define the directory where the plots will be saved
output_dir = 'plots'
sub_dir = 'DeltaPt_Pt2_Distributions'

# Create the directory if it does not exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
# create the sub-directory if it does not exist
sub_dir_path = os.path.join(output_dir, sub_dir)
if not os.path.exists(sub_dir_path):
    os.mkdir(sub_dir_path)

# List of ROOT files
filelist = glob.glob('/eos/user/g/gasadows/Output/TrackingPerformance/Analysis/test/mu*.root')

sigma_DeltaPt_Pt2_list = []
theta_list = []
momentum_list = []

for file_name in filelist:
    # Open ROOT file and get events tree
    file = ROOT.TFile.Open(file_name)
    tree = file.Get("events")

    # Create list sigma_DeltaPt_Pt2 containing the sigma value of the distribution DeltaPt_Pt2
    sigma_DeltaPt_Pt2 = []
    DeltaPt_Pt2 = []
    MC_theta_list = []  # Create empty list for MC_theta values
    MC_p_list = []  # Create empty list for MC_p values
    for i in range(tree.GetEntries()):
        tree.GetEntry(i)
        reco_tlv = tree.Reco_tlv
        MC_tlv = tree.Reco_MC_tlv # MC particle matched to the reco'ed particle
        MC_PDG = tree.Reco_MC_pdg
        for j in range(len(reco_tlv)):
            if (MC_PDG != 0) and (MC_tlv[j].Pt() != 0) and (MC_tlv[j].Theta() != 0) :
                reco_pt = reco_tlv[j].Pt()
                MC_theta = MC_tlv[j].Theta()
                MC_theta_list.append(MC_theta)  # Append each MC_theta value to the list
                MC_p = MC_tlv[j].P()
                MC_p_list.append(MC_p)  # Append each MC_p value to the list
                MC_pt = MC_tlv[j].Pt()
        DeltaPt_Pt2.append( (reco_pt - MC_pt) / (MC_pt * MC_pt) )

    sigma_DeltaPt_Pt2 = np.std(DeltaPt_Pt2)

    # Calculate mean values of theta and momentum
    theta = int(np.mean(np.round(np.rad2deg(MC_theta_list))))  # Calculate mean of reco_theta_list
    momentum = int(np.mean(np.round(MC_p_list)))  # Calculate mean of reco_theta_list

    # Append the values to the lists
    sigma_DeltaPt_Pt2_list.append(sigma_DeltaPt_Pt2)
    theta_list.append(theta)
    momentum_list.append(momentum)

    # Close the ROOT file
    file.Close()

    ############################### Plot the distribution of DeltaPt_Pt2 for each files
    fig, ax = plt.subplots()  # create a new figure and axis object
    file_name = os.path.basename(str(file_name))  # Extract the filename only from the full path
    # Plot the histogram
    n, bins, patches = plt.hist(DeltaPt_Pt2, bins=len(DeltaPt_Pt2), histtype='step')
    # Fit a normal distribution to the data
    mu, std = norm.fit(DeltaPt_Pt2)
    fit_line = scipy.stats.norm.pdf(bins, mu, std) * sum(n * np.diff(bins))
    # Plot the fitted line
    plt.plot(bins, fit_line,'r', linewidth=1, label=(r"$\mu=%0.3e$" + "\n" + r"$\sigma=%0.3e$") % (mu, std))
    plt.xlabel(r'$\Delta p_T / p^2_{T,true}$', fontsize=12)
    plt.title(f'Distribution of $\Delta p_T / p^2_{{T,true}}$ for {file_name}')
    plt.legend()
    plt.savefig(os.path.join(sub_dir_path, f'{file_name}.png'))
    #figure_path = os.path.join(cwd, f'{file_name}.png')
    plt.close(fig)  # close the figure to free up memory
    ###############################

# Rename the lists
theta = theta_list
momentum = momentum_list
sigma_DeltaPt_Pt2 = sigma_DeltaPt_Pt2_list

############################### PLOT DATA ###############################
## Delta_pT vs pT^2 
#----------------------------------
import matplotlib.lines as mlines

fig, ax = plt.subplots() 
ax.set_xscale('log')
ax.set_yscale('log')
#ax.set_title(r'single $\mu^-$', fontsize=14)
ax.set_xlabel(r'$p$ [GeV] ', fontsize=12)
ax.set_ylabel(r'$\sigma(\Delta p_T / p^2_{T,true})$ $[GeV^{-1}] $', fontsize=12)

data_dict = {}
for i in range(len(theta)):
    #if momentum[i] >= 10:
    if theta[i] not in data_dict:
        data_dict[theta[i]] = []
    data_dict[theta[i]].append((momentum[i], sigma_DeltaPt_Pt2[i]))

handles = []
labels = []

for t, data_list in sorted(data_dict.items()):
    ## plot the points by theta
    x, y = zip(*data_list)
    scatter = ax.scatter(x, y, s=30, linewidth=0)
    handles.append(scatter)
    labels.append(r'$\theta$ = '+str(t)+' deg')
    
    ## fit by theta
    p = np.array(x)
    theta_rad = np.deg2rad(t)
    func = lambda p, a, b:a + (b / (p * np.power(np.sin(theta_rad), 3/2))) 
    popt, pcov = curve_fit(func, p, y)
    a, b = popt
    fit_x = np.linspace(min(p), max(p), 10)
    fit_y = func(fit_x, a, b)
    ax.plot(fit_x, fit_y, '--', linewidth=0.5)

legend_line = mlines.Line2D([], [], color='black', linestyle='--', linewidth=0.5)
handles.append(legend_line)
labels.append(r'a + b / (p $\sin^{3/2}\theta)$')

leg = plt.legend(handles,labels, loc='upper right', labelspacing=-0.2, title=r'Single $\mu^-$', title_fontsize='larger')
leg._legend_box.align = "left" # Make title align on the left

# add text in the upper left corner
text_str = "FCC−ee CLD"
plt.text(-0.00005, 1.04, text_str, transform=ax.transAxes, fontsize=12, va='top', ha='left')
# add text in the upper right corner
text_str = "~1000 events/point"
plt.text(1.0, 1.04, text_str, transform=ax.transAxes, fontsize=10, va='top', ha='right')

## Save the plot to a file
plt.savefig(os.path.join(output_dir,'TEST.png'))
#----------------------------------

## show plots
plt.show() 
