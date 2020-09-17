# PMT selection

Main command

```console
./pmt_selection.sh run_min run_max
```

The procedure consists of the following steps:

0. Obtain livetime for each run
1. Construct the list of enabled channels in each run
2. Map the channels to PMTs
3. Select N best PMTs by livetime
4. Obtain average dark noise rate in each PMT
5. Select M best PMTs by dark noise

The numbers N and M are defined on top of the file ```pmt_selection.py```.
Since the procedure might have to be run multiple times depending on the desired run range and the parameters N and M, each step can be commented out if it was already done before to speed up the procedure. Be careful about files being overwritten or the opposite, not replaced if you are re-running the procedure. See details below.

# Notes

## Example 1: different selection

You have done a selection of top 900 PMTs by dark noise out of the top 1000 by livetime in the run range 5007 - 10137.
Now you want to select top 800 PMTs instead.

You can comment out steps 0 (in ```pmt_selection.sh```), 1, 2, 3 and 4 (in ```pmt_selection.py```), since they are common for any final selection and it will save a GREAT deal of time, leaving only step 5 uncommented. Then, in ```pmt_selection.py``` assign the number 800 to the variable M on top of the code.

Then re-run

```console
./pmt_selection.sh 5007 10137
```

or

```console
python2 pmt_selection.py 5007 10137
```

since step 0 contained in ```pmt_selection.sh``` is not needed anyway.

## Example 2: shrink range

You have done a selection of top 900 PMTs by dark noise out of the top 1000 by livetime in the run range 5007 - 10137.
Now you want to change the run range of your selection to 6587 - 12035.

You can skip steps 1 and 2, since the new range is contained within the old, and the information about enabled PMTs would be the same.

Starting from step 3, you have to leave the steps uncommented. They will subselect the given range from the previously saved larger range info and redo the rest of the calculations.

Be careful in this example, if you comment out more than needed, the code will not notify you and will use wrong average dark noise information from the previous larger run range


## Example 3: expand range

You have done a selection of top 900 PMTs by dark noise out of the top 1000 by livetime in the run range 6587 - 10137.
Now you want to change the run range of your selection to 5007 - 12035.

You have to redo the whole procedure, not commenting out anything. However, as you will see, some time will be saved as the table calculated in the previous run range will not all be read from scratch but rather expanded to cover the new range. 


# Procedure

## Step 0: obtain livetime for each run

Python script ```livetime/dst_list.py``` is called from the main script ```pmt_selection.sh``` with the format

```console
python2 livetime/dst_list.py run_min run_max
```

It connects to the database ```bx_runvalidation``` and obtains paths to DSTs in the range between the two given runs from table ```ValidRuns```. The list is saved to ```livetime/dst_list<run_min>-<run_max>.list```.

Then, macro ```livetime/livetime_dst.C``` is called from the main script ```pmt_selection.sh``` with the format


```console
./livetime_dst run_min run_max
```

It reads the DST ROOT files from the list saved before, and extracts the livetime information for each run from the ``live_time``` histograms. The info is saved to file ```livetime/livetime_run<run_min>-<run_max>.csv```.

Afterwards, python script ```pmt_selection.py``` is called from the main script ```pmt_selection.sh```.
This step can be done once for given run_min and run_max and commented out (skipped) if re-running is needed to save time.

Note: livetime is saved into a file that has min and max run in its name. The following step will look for a file with that specific name. If you want to extend your range, the procedure has to be run again to create a new file. If you always work with the same run range, this step can be done once and commented out.

## Step 1: obtain list of enabled channels in each run

The first step is contained in the method ```enabled_channels()``` of the class ```PMTinfo``` (see ```pmt_info.py```). The method is called from ```pmt_selection.py```. The method uses three sources of information:

- livetime: from the file ```livetime/livetime_run<run_min>-<run_max>.csv``` produced in the previous step.
- disabled channels: from the table ```DisabledChannels``` in the ```bx_calib``` database. The method connects to the database and reads the table in case the table is not present in the folder ```livetime/```, or expands it if it is present but does not include desired runs.
- reference channels: from the table ```LabenChannelMapping``` in the ```daq_config``` database. If the table is already present in ```livetime/```, the method connects to the database, reads the table and saves it to speed up time for the next run of the procedure.


For each run present in the livetime file (enabled runs present in DSTs between run_min and run_max), enabled channels are obtained by subtracting disabled and reference channels from all channels. Enabled channels of each run are saved to ```livetime/live_channels_run.csv```.

If a file with such a name is already present, the procedure will first look for it and check if the current run range is contained within the file. In that case, it will simply read it and truncate to the given runs to save time. If the given range is wider, it will do the procedure for the missing runs and update the saved file for the future to save time.


## Step 2: map channels to PMTs

The second step is contained in the method ```map_to_PMT()``` of the class ```PMTinfo``` (see ```pmt_info.py```). It calls the function ```map_lg_to_pmt()``` from ````hole_mapping/map_pmts.py``` giving it as input the result of the previous step. This function uses files present in ```hole_mapping/``` to obtain the mapping profile of each run and assign PMT label to each channel in that run. The resulting table with added PMT info is saved to ```livetime/live_channels_runHoleLabel.csv```.

In case the previous step was skipped, the method reads the file ```livetime/live_channels_run.csv``` and selects the needed run range (if the read one is larger). In case the file does not cover the requested run range, the procedure will stop and ask the user to produce the file with the correct run range.


## Step 3: select top N PMTs by livetime

The third step is contained in the method ```best_N_livetime(N)``` of the class ```PMTinfo``` (see ```pmt_info.py```). It takes as a variable ```N```, the number of PMTs in the selection. The method uses the table produced in the previous step and the livetime information to order PMTs by total livetime in the run range and select top N.

If the previous step was skipped, the method reads the file ```livetime/live_channels_runHoleLabel.csv``` and selects the needed run range (if the read one is larger). In case the file does not cover the requested run range, the procedure will stop and ask the user to produce the file with the correct run range.

The result is saved to the file ```livetime/top<N>pmts_livetime_runs<run_min>-<run_max>.csv```


## Step 4: obtain average dark noise in PMTs

The fourth step is contained in the method ```avg_dark()``` of the class ```PMTinfo``` (see ```pmt_info.py```). It reads the information about dark noise rate in each channel in the given run range from the database, maps channels to PMTs, and calculates average dark noise in each PMT in that run range.
The result is saved to ```dark_noise/DarkNoiseAverage.csv``` along with the intermediate steps.


## Step 5: select top M PMTs by dark noise

The fifth and final step is contained in the method ```best_M_darknoise(M)``` of the class ```PMTinfo``` (see ```pmt_info.py```). It takes as a variable ```M```, the number of PMTs in the selection. The method uses the table produced in step 3 and the dark noise information produced in step 4 to order PMTs by average dark noise rate in the run range and select top M.

In case steps 3 or 4 were skipped, the tables are read from storage. If they do not exist, the user will be prompted to do steps 3 and 4.

The final output are two files:

- ```top<N>pmts_livetime_runs<run_min>-<run_max>_top<M>darknoise.csv```: table with all the information (livetime, dark noise etc)
- ```top<N>pmts_livetime_runs<run_min>-<run_max>_top<M>darknoise.list```: list of the selected PMT labels (hole labels) 

