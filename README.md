# PMT selection

Main command

```console
./pmt_selection run_min run_max
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

## Step 1: obtain list of enabled channels in each run

The first step is contained in the method ```enabled_channels()``` of the class ```PMTinfo``` (see ```pmt_info.py```). The method is called from ```pmt_selection.py```. The method uses three sources of information:

- livetime: from the file ```livetime/livetime_run<run_min>-<run_max>.csv``` produced in the previous step.
- disabled channels: from the table ```DisabledChannels``` in the ```bx_calib``` database. The method connects to the database and reads the table in case the table is not present in the folder ```livetime/```, or expands it if it is present but does not include desired runs.
- reference channels: from the table ```LabenChannelMapping``` in the ```daq_config``` database. If the table is already present in ```livetime/```, the method connects to the database, reads the table and saves it to speed up time for the next run of the procedure.


For each run present in the livetime file (enabled runs present in DSTs between run_min and run_max), enabled channels are obtained by subtracting disabled and reference channels from all channels. Enabled channels of each run are saved to ```livetime/live_channels_run.csv```.

## Step 2: select top N PMTs by livetime

# Details

File channel_livetime.py reads
1) ```livetime_run.csv```
2) ```DisabledChannels.csv```
3) ```LabenChannelMapping.csv```

And produces a file called  ```livetime_run_channels.csv```


1) Table ```livetime_run.csv``` was obtained from the file given to me by Sindhu. She produced a file with livetime (in seconds) for each run for some of her studies. This livetime already accounts for deadtime after the muon cut. As I understand, this information can be extracted from dsts, which contain histograms livetime VS run.

2) File ```DisabledChannels.csv``` is obtained from the table ```DisabledChannels``` in the database ```bx_calib``` using a script ```get_table.sh``` with

```bash
name="DisabledChannels"
query="select \"RunNumber\", \"ChannelID\" from \"$name\" ....."
```

3) File ```LabenChannelMapping.csv``` is obtained from the table ??? in the database ??? using a script ```get_table.sh``` with

```bash
name="LabenChannelMapping"
query="select * from \"$name\" where \"ProfileID\" > 14"
```
