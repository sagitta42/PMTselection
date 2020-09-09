# PMT selection

```console
python pmt_selection.py
```

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
