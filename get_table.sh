# read from database (replace by own command)
name="HolesMapping"
query="select \"HoleLabel\", \"ChannelID\" from \"$name\" where \"ProfileID\" = 25"
echo "$query"
psql -h bxdb.lngs.infn.it -U borex_guest -d bx_calib -F, --no-align --field-separator ',' --pset footer=off -c "$query" > $name.csv 
