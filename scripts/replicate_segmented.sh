#!/bin/sh
##replicate_segmented.sh
##By: Curtis Bennett
##    Vertica Professional Services
##


##Step 1. Run this
##--query to find all the small segmented tables (less than 500,000 rows)
##--and output a set of export_objects commands that will generate all their DDL

##select 'select export_objects(''''' || ',' || '''' ||  proj || ''');'
##from ( select p.projection_schema || '.' || p.anchor_table_name as proj, sum(ps.ros_row_count) as row_Count, count(distinct ps.projection_name) as proj_count
##from projections p, projection_storage ps where p.projection_name = ps.projection_name
  ##and p.projection_schema = ps.projection_schema and ps.ros_row_count > 0 and p.is_segmented
##group by 1 ) pps where row_count/proj_count < 500000 order by proj ;

##Step 2. Take the output of the above, and run that.
##Step 3. Take the output from step 2, and pass it into this script.

DDL=$1

if [ $# -eq 0 ]
then
  echo "You need to supply a file containing CREATE PROJECTION syntax."
else

##remove all the dashed lines from the file
cat $DDL |grep -v "^-------------" > $DDL.1

##remove the rows that say "export_objects"
cat $DDL.1 |grep -v " export_objects " > $DDL.2

##remove the rows that say "(1 row)"
cat $DDL.2 |grep -v "^([0-9] row)" > $DDL.3

##remove any table comments. Grupo Modelo has a lot of those.
cat $DDL.3 |grep -v "^COMMENT ON" > $DDL.4

##remove the ksafe commands
cat $DDL.4 |grep -v "^SELECT MARK_DESIGN_KSAFE" > $DDL.5

##the file contains CREATE TABLE commands
##running them again would just cause them to fail.
##but they can be removed easily enough with some clever sed
##-- give me everything that starts with CREATE PROJECTION
##-- down to ALL NODES KSAFE. That will exclude all the table DDL.
sed -n '/^CREATE PROJECTION/,/ALL NODES KSAFE/p' $DDL.5 > $DDL.6

##replace all the SEGMENTED BY statements with an UNSEGMENTED statement
sed "s/^SEGMENTED BY.*/UNSEGMENTED ALL NODES;/" $DDL.6 > $DDL.7

##Vertica includes projection creation comments, which we can use to
##our advantage, since I need to change the projection names
sed "s/ [/][*]+createtype/_unseg \/*+createtype/" $DDL.7 > $DDL.8

##housecleaning
rm -f $DDL.1
rm -f $DDL.2
rm -f $DDL.3
rm -f $DDL.4
rm -f $DDL.5
rm -f $DDL.6
rm -f $DDL.7

mv $DDL.8 $DDL.unsegmented.sql

##Create the DROP PROJECTION DDL.
cat $DDL |grep "^CREATE PROJECTION" > $DDL.9

##replace all the create statements with drop statements
sed "s/^CREATE /DROP /" $DDL.9 > $DDL.10

##replace the projection creation comment with a cascade;
sed "s/ [/][*]+createtype[(][A-Z][)][*][/]/ CASCADE ;/" $DDL.10 > $DDL.11

##a bit more housecleaning
rm -f $DDL.9
rm -f $DDL.10

##create the drop projection file.
echo "SELECT MARK_DESIGN_KSAFE(0);" > $DDL.drop.sql
cat $DDL.11 >> $DDL.drop.sql
echo "SELECT MARK_DESIGN_KSAFE(1);" >> $DDL.drop.sql

rm -f $DDL.11

echo "##############################"
echo Run $DDL.unsegmented.sql
echo Then, assuming there were no errors in the above, run $DDL.drop.sql

fi
