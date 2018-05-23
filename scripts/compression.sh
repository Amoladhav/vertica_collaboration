#!/bin/bash
#Vertica Compression estimator. Run with Vertica administrator credentials.
#For each table, this outputs Projection Count, Projection Segmentation type [R(eplicated), S(egmented), M(ixed)],
#Row Count, Raw Size(MB), Vertica Size (MB), and Compression Ration.
#Usage:
#       ./compression.sh  [-U username -w password] "
#       echo " Use database administrator account for user name

unset USER PASSWORD

args=`getopt U:w: $*`
if test $? != 0
     then
        echo " Usage is ./compression.sh  [-U username -w password] "
        echo " Use database administrator account for user name "
        exit 1
fi
set -- $args

for i
do
  case "$i" in
        -U) shift;USER=$1;shift;;
        -w) shift;PASSWORD=$1;shift;;
  esac
done

if [ "$USER" != "" ]; then
        us="-U $USER"
fi

if [ "$PASSWORD" != "" ]; then
        pw="-w $PASSWORD"
fi

#initialize some variables
sum_rawbytes="0"
sum_verticabytes="0"
starttime=`date +'%s'`

        printf "\n\033[33mVertica Compression estimator. Run with Vertica administrator credentials.\033[0m\n\n"

    printf "PjCnt\t\t=> Projection count\n"
        printf "PjType\t\t=> Projection segmentation type R(eplicated), S(egmented), M(ixed)\n"
        printf "RowCount\t=> Table row count\n"
        printf "RawSize(MB)\t=> Estimated table data raw size reported in MegaBytes\n"
        printf "Vertica(MB)\t=> Reported Vertica compressed total projection size across cluster reported in Megabytes\n"
        printf "Compr.Ratio\t=> Calculated compression ratio as Raw data over Vertica compressed data\n\n\n"

        printf "\033[33m %40s%10s%10s%20s%20s%20s%20s%20s\033[0m\n" "TableName" "PjCnt" "PjType" "RowCount" "RawSize(MB)" "Vertica(MB)" "Compr.Ratio" "Del.Vectrs"
	printf  "\033[33m===================================================================================================================================================================\033[0m\n"
for  line in `vsql $us $pw -tAX -c "SELECT anchor_table_schema || '.' || anchor_table_name AS table
     , SUM(used_bytes) AS bytes
     , MAX(row_count) AS row_count
     , COUNT(DISTINCT PJNAME_SHORT) AS pjcount
     , CASE WHEN MIN(PJTYPE) <> MAX(PJTYPE) THEN 'M' ELSE MIN(PJTYPE) END AS pjtype
     , MAX(foo.deleted_row_count) as deleted_row_count
  FROM (SELECT ps.anchor_table_schema, ps.anchor_table_name, ps.projection_name
            , CASE p.is_segmented WHEN true then SUM(used_bytes) ELSE avg(used_bytes)::int - COALESCE(MAX(dv.deleted_used_bytes), 0) END AS used_bytes
            , CASE p.is_segmented WHEN true THEN ps.projection_name
              ELSE SPLIT_PART( ps.projection_name , '_node' , 1 ) END PJNAME_SHORT
            , CASE p.is_segmented WHEN true THEN 'S' ELSE  'R' END PJTYPE
            , CASE p.is_Segmented WHEN true THEN sum(row_count) ELSE max(row_count) END - COALESCE(MAX(dv.deleted_row_count), 0) row_count
            , MAX(dv.deleted_row_count) as deleted_row_count
         FROM projection_storage ps
         JOIN projections p USING (projection_id)
         LEFT JOIN (SELECT schema_name, projection_name
                         , SUM(deleted_row_count) as deleted_row_count
                         , SUM(used_bytes) as deleted_used_bytes
                      FROM delete_vectors
                     GROUP BY schema_name, projection_name) dv
           ON dv.schema_name = ps.anchor_table_schema
          AND dv.projection_name = ps.projection_name
        GROUP BY 3,1,2,is_segmented ) AS foo
 GROUP BY foo.anchor_table_schema, foo.anchor_table_name
 ORDER BY 2 DESC;"`
do
        tbname=`echo $line | awk ' BEGIN{FS="|"}  {print $1}'`
      # vertica_bytes=`echo $line | awk ' BEGIN{FS="|"}  {print $2/1048576}'` #store in MB
        vertica_bytes=`echo $line | awk ' BEGIN{FS="|"}  {printf "%.2f",$2/1048576}'` #store in MB
        row_count=`echo $line  | awk ' BEGIN{FS="|"}  {print $3}'`

        projcount=`echo $line  | awk ' BEGIN{FS="|"}  {print $4}'` # account for # of projections per table, replicated counts as one but bytes are x nonodes
        projtype=`echo $line  | awk ' BEGIN{FS="|"}  {print $5}'` # this could be R-replicated , S-Segmented , M-mixed


        if [ $row_count -gt 0 ]; then
                bytesperrow=`vsql $us $pw -AtX -c "select * from $tbname  where random() < 0.05 limit 1000000" | wc -lc |  awk  ' BEGIN{FS=" "}  { if ($1 != 0 ) print $2/$1; else print 0 }'`
                raw_bytes=`echo "scale=10; $bytesperrow * $row_count / 1048576" | bc`
                                if [ "$vertica_bytes" != "0.00" ]; then
                                        compression=`echo "scale=10;  $raw_bytes / $vertica_bytes" | bc`
                                else
                                        compression=""
                                fi
        else
                raw_bytes="0"
                compression=""
        fi

        deletedcount=`echo $line  | awk ' BEGIN{FS="|"}  {print $6}'` # this is the number of rows in the delete_vectors table

        printf " %40s%10s%10s%20s%20.2f%20.2f%20.2f%20s\n" $tbname $projcount $projtype $row_count $raw_bytes  $vertica_bytes $compression $deletedcount

        # keep running counts
        sum_rawbytes=`echo "scale=10;  $sum_rawbytes + $raw_bytes" | bc`
        sum_verticabytes=`echo "scale=10;  $sum_verticabytes + $vertica_bytes" | bc`

done


        overallcompression=`echo "scale=10;  $sum_rawbytes / $sum_verticabytes" | bc`
        printf  "\033[33m___________________________________________________________________________________________________________________________________________________________________\033[0m\n"
        printf  "\033[33m %40s%10s%10s%20s%20.2f%20.2f%20.2f\033[0m\n" "TOTAL" "" "" "" $sum_rawbytes $sum_verticabytes $overallcompression

        currenttime=`date +'%s'`
        printf "\t\tElapsed time %d (secs)\n" $((currenttime - starttime))



