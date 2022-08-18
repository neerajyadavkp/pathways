set -x
#!/bin/bash
############################################################################################
# Project: MENAT Pathways - Phase 1
# Region : AMEA
# Purpose: This script is used to orchestrate data loads. This is the script called from NiFi.
############################################################################################

# =====================================================================
# FileName: Orchestator.sh
# # =====================================================================
# Objective: Managing Data Load of Actuals ,Forecast, OS and Arrivals
#
# Workflow of Script is to call the respective shell scripts  (Actuals ,Forecast, OS Or Arrivals) based on the input parameter
#   
#
# 
# Parameters
#
# $2 = Shared file PATH                \\usphi544\datacov\USER_INPUT\amea\MENAT_KSOP     			
# $1 = LOG DIRECTORY LOCATION           /home/inca1n04
# $2 = Absolute ath of the file in the shared folder
# $3 = Filename of the file recieved
# $4 = Shell script directory path
# $5 = Shell script which merge/insert overwrite actuals
# $6 = Hive script path 
# $7 = Shell script which merge/insert overwrite forecast
# $8 = Shell script which merge/insert overwrite Os
# $9 = Shell script which merge/insert overwrite Arrivals
#
#
# Author                          Date                     Comment
# ---------------------------------------------------------------------------------
# Antony Nadan                 1/27/2021                 First version of the script created

echo "############ Orchestrator Process start time $(date +'%d%m%Y%H%M%S') ############ "
SCRIPT=$(readlink -f $0)
SOURCE_CODEPATH=`dirname $SCRIPT | sed 's!\(.*\)/.*!\1!'`
echo ${SOURCE_CODEPATH}
source $SOURCE_CODEPATH/config
if [ $? -eq 0 ]
then
  echo "### Config file loaded"
else
  echo "### Error loading config file"
  exit 4
fi


#######################Initialize arguments############################

log_dir=${MPW_LOG_DIR}
absolutepath=$1
filename=$2
shellScriptDir=${MPW_SHELL_DIR}
pythonScriptDir=${MPW_PYTHON_SCRIPT_DIR}
hiveScriptPath=${MPW_HQL_DIR}
outputFolder=${MPW_OUTPUT_DIR}
outputFolderDaily=${MPW_DAILY_OUTPUT_DIR}
hdfs_actual_src=${MPW_HDFS_ACTUAL_SRC}
hdfs_actual_archive=${MPW_HDFS_ACTUAL_ARC}
hdfs_actual_daily_src=${MPW_HDFS_ACTUAL_DAILY_SRC}
hdfs_actual_daily_archive=${MPW_HDFS_ACTUAL_DAILY_ARC}
hdfs_target_src=${MPW_HDFS_TARGET_SRC}
hdfs_target_archive=${MPW_HDFS_TARGET_ARC}
hdfs_customer_src=${MPW_HDFS_CUSTOMER_SRC}
hdfs_customer_archive=${MPW_HDFS_CUSTOMER_ARC}
hdfs_material_src=${MPW_HDFS_MATERIAL_SRC}
hdfs_material_archive=${MPW_HDFS_MATERIAL_ARC}
hdfs_forecast_src=${MPW_HDFS_FORECAST_SRC}
hdfs_forecast_archive=${MPW_HDFS_FORECAST_ARC}
hdfs_OS_src=${MPW_HDFS_OS_SRC}
hdfs_OS_archive=${MPW_HDFS_OS_ARC}
hdfs_Arrivals_src=${MPW_HDFS_ARRIVAL_SRC}
hdfs_Arrivals_archive=${MPW_HDFS_ARRIVAL_ARC}
pythonrptHDFS=${MPW_HDFS_IPR}
adj_hdf=${MPW_HDFS_ADJ}


mergeActuals=${MPW_HQL_MERGE_ACTUAL}
mergeActualsDaily=${MPW_HQL_MERGE_ACTUAL_DAILY}
mergeTarget=${MPW_HQL_MERGE_TARGET}
mergeCustomer=${MPW_HQL_MERGE_CUSTOMER}
mergeMaterial=${MPW_HQL_MERGE_MATERIAL}
mergeForecast=${MPW_HQL_MERGE_FORECAST}
mergeOS=${MPW_HQL_MERGE_OS}
mergeArrivals=${MPW_HQL_MERGE_ARRIVALS}


#######################################################################




######################START LOG ATTRIBUTE ###########################
run_id=$(date +%F%T|sed 's/-//g'|sed 's/://g')
LOAD_DATE=$(date +%Y%m%d)
load_dir=$log_dir/Orchestrator/$LOAD_DATE

if [ ! -d $load_dir ] ; then
mkdir -p $load_dir
chmod -R 777 $load_dir
fi

load_log=$load_dir/orchestrator_log_$run_id.log
######################END LOG ATTRIBUTE ############################

echo "hiveScriptPath is ############################:" $hiveScriptPath 2>&1 >> $load_log


#################### Regex to get the file name########################


SUB_actuals="MPW_IMS_Actual_IN"
SUB_actuals_daily="MPW_IMS_Daily_Actual_IN"
SUB_target="TargetMapping"
SUB_customer="CustomerMapping"
SUB_material="MaterialMapping"
SUB_os="MPW_IMS_OS_IN"
SUB_web="MPW_IMS_WEB_IN"
SUB_forecast="MPW_IMS_Forecast_IN"

result=''

if [[ "$filename" =~ .*"$SUB_actuals".* ]]; then
  result='actuals'
fi
if [[ "$filename" =~ .*"$SUB_actuals_daily".* ]]; then
  result='daily'
fi
if [[ "$filename" =~ .*"$SUB_target".* ]]; then
  result='target'
fi
if [[ "$filename" =~ .*"$SUB_customer".* ]]; then
  result='customer'
fi
if [[ "$filename" =~ .*"$SUB_material".* ]]; then
  result='material'
fi
if [[ "$filename" =~ .*"$SUB_os".* ]]; then
  result='os'
fi
if [[ "$filename" =~ .*"$SUB_web".* ]]; then
  result='web'
fi
if [[ "$filename" =~ .*"$SUB_forecast".* ]]; then
  result='forecast'
fi
echo "File uploaded is $result"

###################### Done with filename regex ############################

echo "the resulting file is ############################:" $result  2>&1 >> $load_log



case $result in
   "actuals") 
	
       
        insertOWActuals=$hiveScriptPath/$mergeActuals
	
        
    	echo "The shell script for actuals is present in :" $shellScriptDir/Worker.sh 2>&1 >> $load_log
		echo "The absoulute path of the file in shared location is:" $absolutepath  2>&1 >> $load_log
		echo "The log location for actuals is :" $log_dir 2>&1 >> $load_log
		echo "The HDFS SRC for actuals :" $hdfs_actual_src 2>&1 >> $load_log
		echo "The HDFS Archive for actuals :" $hdfs_actual_archive 2>&1 >> $load_log
		echo "The filename is :" $filename 2>&1 >> $load_log
		echo "insertOWActuals  :"$insertOWActuals 2>&1 >>  $load_log
		echo "outputFolder :"$outputFolder 2>&1 >>  $load_log
		echo "pythonScriptDir :"$pythonScriptDir 2>&1 >>  $load_log
		echo "hiveScriptPath :"$hiveScriptPath 2>&1 >>  $load_log
		echo "pythonrptHDFS :"$pythonrptHDFS 2>&1 >>  $load_log
		echo "adj_hdf:"$adj_hdf 2>&1 >>  $load_log
		echo "beelineConString:" $beelineConString 2>&1 >>  $load_log
	

	$shellScriptDir/mpw_worker.sh $absolutepath $log_dir $hdfs_actual_src $hdfs_actual_archive $filename $insertOWActuals $outputFolder $pythonScriptDir $hiveScriptPath $pythonrptHDFS $adj_hdf "" $result $shellScriptDir

        echo "Done processing the actual file" 2>&1 >>  $load_log

   ;;
   "forecast") 
	
	insertOWforecast=$hiveScriptPath/$mergeForecast
	        
    	echo "The shell script for forecast is present in :" $shellScriptDir/Worker.sh 2>&1 >> $load_log
		echo "The absoulute path of the file in shared location is:" $absolutepath  2>&1 >> $load_log
		echo "The log location for forecast is :" $log_dir 2>&1 >> $load_log
		echo "The HDFS SRC for forecast :" $hdfs_forecast_src 2>&1 >> $load_log
		echo "The HDFS Archive for forecast:" $hdfs_forecast_archive 2>&1 >> $load_log
		echo "The filename is :" $filename 2>&1 >> $load_log
		echo "insertOWforecast :"$insertOWforecast2>&1 >> $load_log
		echo "outputFolder :"$outputFolder 2>&1 >> $load_log
		echo "pythonScriptDir :"$pythonScriptDir 2>&1 >> $load_log
		echo "hiveScriptPath :"$hiveScriptPath 2>&1 >> $load_log
		echo "pythonrptHDFS :"$pythonrptHDFS 2>&1 >> $load_log
		echo "adj_hdf:"$adj_hdf 2>&1 >> $load_log
		echo "beelineConString:" $beelineConString 2>&1 >> $load_log

       
	$shellScriptDir/mpw_worker.sh $absolutepath $log_dir $hdfs_forecast_src $hdfs_forecast_archive $filename $insertOWforecast $outputFolder $pythonScriptDir $hiveScriptPath $pythonrptHDFS $adj_hdf "" $result $shellScriptDir



    echo "Done processing the forecast file" 2>&1 >> $load_log 

   ;;
   "os")  

	insertOWOS=$hiveScriptPath/$mergeOS
       
    echo "The shell script for OS is present in :" $shellScriptDir/Worker.sh 2>&1 >> $load_log
	echo "The absoulute path of the file in shared location is:" $absolutepath  2>&1 >> $load_log
	echo "The log location for OS is :" $log_dir 2>&1 >> $load_log
	echo "The HDFS SRC for OS :" $hdfs_OS_src 2>&1 >> $load_log
	echo "The HDFS Archive for OS:" $hdfs_OS_archive 2>&1 >> $load_log
	echo "The filename is :" $filename 2>&1 >> $load_log
	echo "insertOWOS  :"$insertOWOS 2>&1 >> $load_log
	echo "outputFolder :"$outputFolder 2>&1 >> $load_log
	echo "pythonScriptDir :"$pythonScriptDir 2>&1 >> $load_log
	echo "hiveScriptPath :"$hiveScriptPath 2>&1 >> $load_log
	echo "pythonrptHDFS :"$pythonrptHDFS 2>&1 >> $load_log
	echo "adj_hdf:"$adj_hdf 2>&1 >> $load_log
	echo "beelineConString:" $beelineConString 2>&1 >> $load_log

	$shellScriptDir/mpw_worker.sh $absolutepath $log_dir $hdfs_OS_src $hdfs_OS_archive $filename $insertOWOS $outputFolder $pythonScriptDir $hiveScriptPath $pythonrptHDFS $adj_hdf "" $result $shellScriptDir



    echo "Done processing the OS file" 2>&1 >> $load_log

   ;;
   "web") 

	insertOWWeb=$hiveScriptPath/$mergeArrivals
       
    echo "The shell script for OS is present in :" $shellScriptDir/Worker.sh 2>&1 >> $load_log
	echo "1.The absoulute path of the file in shared location is:" $absolutepath  2>&1 >> $load_log
	echo "2.The log location for Web is :" $log_dir 2>&1 >> $load_log
	echo "3.The HDFS SRC for Web :" $hdfs_Arrivals_src 2>&1 >> $load_log
	echo "5The HDFS Archive for Web:" $hdfs_Arrivals_archive 2>&1 >> $load_log
	echo "6.The filename is :" $filename 2>&1 >> $load_log
	echo "7.insertOWWeb:"$insertOWWeb 2>&1 >> $load_log
	echo "8.outputFolder :"$outputFolder 2>&1 >> $load_log
	echo "9.pythonScriptDir :"$pythonScriptDir 2>&1 >> $load_log
	echo "10.hiveScriptPath :"$hiveScriptPath 2>&1 >> $load_log
	echo "11.pythonrptHDFS :"$pythonrptHDFS 2>&1 >> $load_log
	echo "12.adj_hdf:"$adj_hdf 2>&1 >> $load_log
	echo "beelineConString:" $beelineConString 2>&1 >> $load_log

       
	$shellScriptDir/mpw_worker.sh $absolutepath $log_dir $hdfs_Arrivals_src $hdfs_Arrivals_archive $filename $insertOWWeb $outputFolder $pythonScriptDir $hiveScriptPath $pythonrptHDFS $adj_hdf "" $result $shellScriptDir


    echo "Done processing the Web file" 2>&1 >> $load_log

   ;;
esac

echo "############ Orchestrator Process End time $(date +'%d%m%Y%H%M%S') ############ "