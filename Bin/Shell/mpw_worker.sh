set -x
#!/bin/bash
#!/bin/sh
exec 3>&1 4>&2
trap 'exec 2>&4 1>&3' 0 1 2 3
exec 1>worker.log 2>&1

############################################################################################
# Project: MENAT Pathways - Phase 1
# Region : AMEA
# Purpose: This script is used to load data to Hive tables and execute the python scripts
############################################################################################
# =====================================================================
# FileName: Worker.sh
# # =====================================================================
# Objective: Managing Data Load of Actuals to SRC,PCD and ARCHIVE 
#
# A. Workflow of Script
#
# step 1: Copy Source File from shared folder to Archive Folder
# step 2: Remove file in SRC folder
# Step 3: Copy Source File from shared folder to SRC Folder
# Step 4: Delete file from shared folder
# Step 5: Run Python script for Pipeline report
#
#
# Parameters
#
#   $1 - Absolute path of the file
#   $2 - log_dir 
#   $3 - HDFS src location
#   $4 - HDFS archive location
#   $5 - File name 
#   $6 - Insert OverWrite Script path 
#   $7 - Extraction location 
#   $8 - python Script Path location  
#   $9 - Hive Script Path 
#   $10- python report HDFS 
#   $11- adj HDFs location is 
#   $12- beelineConString 
# 
#
# Author                          Date                     Comment
# ---------------------------------------------------------------------------------
# Antony Nadan                 1/27/2021                 First version of the script created
# Anand Tj                     30/04/2021                Add global trap and change HQL execution

echo "############ Worker Process start time $(date +'%d%m%Y%H%M%S') ############ "
SCRIPT=$(readlink -f $0)
SOURCE_CODEPATH=`dirname $SCRIPT | sed 's!\(.*\)/.*!\1!'`
echo ${SOURCE_CODEPATH}
source $SOURCE_CODEPATH/config
if [ $? -eq 0 ]
then
  echo "### Config file loaded in Worker"
else
  echo "### Error loading config file in Worker"
  exit 4
fi
######################START INITIALIZE PARAMETERS ################
absoluteFilePath=$1
log_dir=$2
src=$3
archive=$4
fileName=$5
insertOverWrtfile=$6
extractLocation=$7
pythonScriptPath=$8
HiveScriptPath=$9
pythonrptHDFS=${10}
adj_hdf=${11}
beelineConString=${12}
logFolderName=${13}
shellScriptDir=${14}


Category=$logFolderName
echo "****************** File uploaded is $Category ***************************" >> $load_log

extractionLocSharedFolder=${MPW_HIVE_EXPORT_LOC}

extractForecast_Latest_hql=$HiveScriptPath/${MPW_HQL_PCD_FORECAST}
extractActuals_All_hql=$HiveScriptPath/${MPW_HQL_PCD_ACTUALS}
extractOs_All_hql=$HiveScriptPath/${MPW_HQL_PCD_OS}
extractweb_All_hql=$HiveScriptPath/${MPW_HQL_PCD_WEB}
extractAdj_All_hql=$HiveScriptPath/${MPW_HQL_PCD_ADJ}
insertOverWrtPipelineInput=$HiveScriptPath/${MPW_HQL_PCD_IPR}
extractLeftover_All_hql=$HiveScriptPath/${MPW_HQL_PCD_LEFTOVER}
python_path=${MPW_PYTHON_PATH}
######################END INITIALIZE PARAMETERS ##################



######################START LOG ATTRIBUTE ###########################
run_id=$(date +%F%T|sed 's/-//g'|sed 's/://g')
LOAD_DATE=$(date +%Y%m%d)
load_dir=$log_dir/$logFolderName/$LOAD_DATE

if [ ! -d $load_dir ] ; then
mkdir -p $load_dir
chmod -R 777 $load_dir
fi

load_log=$load_dir/$logFolderName_log_$run_id.log
######################END LOG ATTRIBUTE ###########################


echo "Below is the list of arguments recieved from orchestrator" >> $load_log
echo "1.Absolute path:" $absoluteFilePath >> $load_log
echo "2.log_dir:" $log_dir>> $load_log
echo "3.HDFS src :" $src >> $load_log
echo "4.HDFS archive :" $archive >> $load_log
echo "5.File name is :" $fileName >> $load_log
echo "6.Insert OverWrite Scipt path :" $insertOverWrtfile >> $load_log
echo "7.Extraction location is :" $extractLocation >> $load_log
echo "8.pythonScriptPathlocation is :" $pythonScriptPath >> $load_log
echo "9.HiveScriptPath is :" $HiveScriptPath >> $load_log
echo "10.pythonrptHDFS is :" $pythonrptHDFS >> $load_log
echo "11.adjis :" $adj_hdf >> $load_log
echo "12.beelineConString :" $beelineConString >> $load_log
echo "13.Category :" $Category>> $load_log




################Start of File Deletion in HDFS SRC location################
echo "The file in the HDFS SRC location being deleted......" >> $load_log

DeleteFromSRC()
{
hdfs  dfs -test -e  $src
local value=$?
#echo $value
if [ $value == 0 ]
then 
	echo "File exists" >> $load_log
	hdfs  dfs -rm -f $src/*
fi
}

DeleteFromSRC
echo "The file in the HDFS source location is deleted" >> $load_log
################End of File Deletion in HDFS SRC location################



############### Start of Copying the file from shared location to HDFS SRC#####################
echo "The file in the shared location is being copied to the HDFS SRC location......" >> $load_log
echo 'The command exectuted is : hdfs dfs -put' $absoluteFilePath $src >>$load_log
hdfs dfs -put $absoluteFilePath $src
echo $? >> $load_log
echo hdfs dfs -put $absoluteFilePath $src >>$load_log
echo "The file in the shared location is copied to the HDFS SRC location" >> $load_log
############### End of Copying the file from shared location to SRC#####################



###############Start of Copying the file from shared location to HDFS Archive##################
echo "The file in the shared location is being copied to the HDFS Archive location......:" $absoluteFilePath >> $load_log
echo 'The command exectuted is : hdfs dfs -put' $absoluteFilePath $archive >> $load_log
hdfs dfs -put $absoluteFilePath $archive
echo "The file in the shared location is copied to the HDFS Archive location" >> $load_log
###############End of Copying the file from shared location to HDFS Archive##################


###############Rename the file copied in HDFS Archive location ############################
echo "The file in the HDFS Archive location is being renamed......" >> $load_log
echo $fileName >> $load_log
today=`date '+%Y%m%d__%H%M%S'`;
NewfileName=$fileName_$today.csv
echo "The file in archive :" $archive/$fileName.csv >> $load_log
echo "The archive file is remaned to :" $archive/$NewfileName >> $load_log
echo "The command exectuted is : hdfs dfs -mv" $archive/$fileName $archive/$NewfileName >> $load_log

hdfs dfs -mv $archive/$fileName $archive/$NewfileName
echo "The file in the HDFS Archive location is renamed" >> $load_log
###############Rename the file copied in HDFS Archive location ############################


###########################Insert overwrite to the PCD table (hive command)##################
echo "Start Insert overwrite to the PCD table (hive command).." >> $load_log
echo "The insert overwrite query is  run from :" $insertOverWrtfile >> $load_log

typeset beelineConString=$beelineConString
typeset hiveScriptPath=$insertOverWrtfile
echo "hiveScriptPath ...:" $hiveScriptPath >> $load_log

${BEELINECONNECT} -f ${hiveScriptPath} --verbose >> $load_log

echo "End Insert overwrite to the PCD table (hive command).." >> $load_log
############################################################################################

####################### SAP extracts if Web file is received ##############################
if [ $Category == 'web' ]
then 
	echo "SAP extracts when we receive Web file" >> $load_log
	$shellScriptDir/${MPW_SHELL_SAP_EXTRACT}	
fi

###########################################################################################



############################Delete the file in the  shared location#######################
echo "Start delete file in the shared location..." >> $load_log
rm -f $absoluteFilePath
echo "End delete file in the shared location.." >> $load_log
############################Delete the file in the  shared location#######################

echo '############## forecast(latest) ,actuals (all) ,os (all) and web (all) -- PCD tables ##############################' >> $load_log
if [ $Category == 'forecast' ]
then 
	echo "Forecast file uploaded."
	echo "############## Extraction of forecast(latest) started...##############:" $extractForecast_Latest_hql >> $load_log
	typeset forecast_lastest_path=$extractForecast_Latest_hql
	forecast_Text=`cat $forecast_lastest_path`
	#echo "$forecast_Text" >> $load_log
	${BEELINECONNECT} -f $extractForecast_Latest_hql  --outputformat=csv2  --verbose > $extractLocation/16_MPW_IMS_Forecast_OUT.csv; 
	 
	echo "############## Extraction of actuals (all) started...############## :" $extractActuals_All_hql >> $load_log
	typeset actuals_script_path=$extractActuals_All_hql
	actuals_Text=`cat $actuals_script_path`
	#echo "$actuals_Text" >> $load_log
	${BEELINECONNECT} -f $extractActuals_All_hql --outputformat=csv2  --verbose > $extractLocation/15_MPW_IMS_Actual_OUT.csv; 


	echo "############## Extraction of web (all) started...##############:" $extractweb_All_hql >> $load_log
	typeset web_script_path=$extractweb_All_hql
	web_Text=`cat $web_script_path`
	echo "$web_Text" >> $load_log
	#echo ${beelineConString} --outputformat=csv2 -e 'use amea;' "$web_Text" > $extractLocation/18_MPW_IMS_WEB_OUT.csv; >> $load_log
	${BEELINECONNECT} -f $extractweb_All_hql --outputformat=csv2 --verbose > $extractLocation/18_MPW_IMS_WEB_OUT.csv; 


	echo "############## Extraction of os (all) started...############## :" $extractOs_All_hql >> $load_log
	typeset os_script_path=$extractOs_All_hql
	os_Text=`cat $os_script_path`
	#echo "$os_Text" >> $load_log
	${BEELINECONNECT} -f $extractOs_All_hql --outputformat=csv2 --verbose > $extractLocation/17_MPW_IMS_OS_OUT.csv; 

	echo "############## Extraction of adj (all) started...############## " >> $load_log
	typeset adj_script_path=$extractAdj_All_hql
	adj_Text=`cat $adj_script_path`
	#echo "$adj_Text" >> $load_log
	${BEELINECONNECT} -f $extractAdj_All_hql --outputformat=csv2 --verbose > $extractLocation/24_MPW_IMS_ADJ_OUT.csv; 

	echo "############## Extraction of leftover started...############## " >> $load_log
	typeset leftover_script_path=$extractLeftover_All_hql
	leftOver_Text=`cat $leftover_script_path`
	#echo "$adj_Text" >> $load_log
	${BEELINECONNECT} -f $extractLeftover_All_hql --outputformat=csv2 --verbose > $extractLocation/27_MPW_Leftovers_OUT.csv; 
        wait
	echo '###########################Done extracting csv from queries#################################################' >> $load_log
     

	echo "############## Extraction of PipelineReport...############## " >> $load_log
	typeset pipeline_report_path=$HiveScriptPath/LastMonth_pipelineReport.hql
	pipelinePath_Text=`cat $pipeline_report_path`
	#echo "$pipelinePath_Text" >> $load_log
	${BEELINECONNECT} -f $HiveScriptPath/LastMonth_pipelineReport.hql --outputformat=csv2 --verbose > $extractLocation/20_MPW_PipelineLM_OUT.csv;
	echo "############## Done Extraction of PipelineReport############## " >> $load_log



	echo '#################### Python scripts to be executed(commented for now as issue fix is in progress)########################' >> $load_log
	echo '#################### The python scripts are executed from here ########################'$pythonScriptPath >> $load_log

        echo "Starting Leftover Processing" >> $load_log
	$python_path $pythonScriptPath/${MPW_PY_LEFTOVER} >> $load_log
	echo "Starting SAP FILE Processing" >> $load_log
	$python_path $pythonScriptPath/${MPW_PY_SAP_MERGE} >> $load_log
	echo "Starting IPR Processing" >> $load_log
	$python_path $pythonScriptPath/${MPW_PY_IPR} >> $load_log
	echo '#################### Done executing python script ########################' >> $load_log


	echo "################Start of File Deletion in HDFS Python report location################"
	echo "The file in the HDFS SRC location being deleted......" >> $load_log

	DeleteFromPythonReport()
	{
	hdfs  dfs -test -e  $pythonreport
	local value=$?
	#echo $value
	if [ $value == 0 ]
	then 
		echo "File exists" >> $load_log
		hdfs  dfs -rm -f $pythonreport/*
	fi
	}

	DeleteFromPythonReport
	echo "The file in the HDFS python_report location is deleted" >> $load_log
	echo "################End of File Deletion in HDFS python_report location################"






	echo'####################Copy adj_output.csv to HDFS#####################' >> $load_log
	adjfile=$extractLocation/25_MPW_ADJ_LM_Python.csv


	DeleteSrcAdj()
	{
	hdfs  dfs -test -e  $adj_hdf
	local value=$?
	#echo $value
	if [ $value == 0 ]
	then 
		echo "File exists" >> $load_log
		hdfs  dfs -rm -f $adj_hdf/*
	fi
	}

	DeleteSrcAdj

	hdfs dfs -put $adjfile $adj_hdf
	echo '####################Copy adj_output.csv to HDFS Done########################' >> $load_log




	###########################Insert overwrite Adjustment)##################
	adj_overwrite=$HiveScriptPath/${MPW_HQL_MERGE_ADJ}
	echo "Start Insert overwrite to the PCD table (hive command).." >> $load_log

	typeset hiveScriptPath=$adj_overwrite
	echo "hiveScriptPath ...:" $hiveScriptPaths >> $load_log

	${BEELINECONNECT} -f ${adj_overwrite}  --verbose 

	echo "Done Insert overwrite Adjustment" >> $load_log
	############################################################################################




	### Delete tablaeu file in hdfs
	DeleteTablaeu()
	{
	hdfs  dfs -test -e  $pythonrptHDFS
	local value=$?
	echo $value
	if [ $value == 0 ]
	then 
		echo "File exists" >> $load_log
			echo "$pythonrptHDFS" >> $load_log
		hdfs  dfs -rm -f $pythonrptHDFS/*
			echo $? >> $load_log
	fi
	}
	DeleteTablaeu



	echo '####################Copy the tableau file to python report tables HDFS location  #####################' >> $load_log
	#tabaleufile=$extractLocation/8_MPW_Pipeline_Python.csv

	tableau_filename_exp='8_MPW_Pipeline_Python'
	tabaleufile="$(ls  $extractLocation/$tableau_filename_exp*)"

	echo 'The tableau file to be deleted is:' $tabaleufile >> $load_log
	hdfs dfs -put $tabaleufile $pythonrptHDFS

	echo '### delete the8_MPW_Pipeline_Python  file ###' >> $load_log
	rm -f $tabaleufile
	echo '#################### Copied to tabaleu table########################' >> $load_log


	echo '###########################Insert overwrite to the PipelineInput table (hive command)################## ' >> $load_log
	echo "Start Insert overwrite to the Pipeline table (hive command).." >> $load_log
	echo "The insert overwrite query is  run from : $insertOverWrtPipelineInput" >> $load_log

	typeset pipelineMergeScript_path=$insertOverWrtPipelineInput
	echo "pipelineMergeScript_path...: $pipelineMergeScript_path">> $load_log

	${BEELINECONNECT} -f ${pipelineMergeScript_path}  --verbose 

	echo "End Insert overwrite to the PipelineInput table (hive command).." >> $load_log
	echo '############################################################################################ ' >> $load_log


	echo "##########Call python transformer ############################" >> $load_log
	integratedExtractloc=$extractLocation/IntegratedPipelineExtracts/
	IntegratedPipelineFile=$(find $extractLocation -type f -name '9_MPW*')
	echo $IntegratedPipelineFile >> $load_log
	echo ${IntegratedPipelineFile:2} >> $load_log
	$python_path $pythonScriptPath/${MPW_PY_XLT} --inputFilePath $IntegratedPipelineFile --outputFilePath $integratedExtractloc >> $load_log
	#rm -f $extractLocation/${IntegratedPipelineFile:2} >> $load_log
	echo "##########End of python transformer ############################" >> $load_log

	echo "##########Creating trigger file for KSOP MODEL REPORT ############################" >> $load_log
	touch $extractLocation/KSOP_MODEL_TRIGGER
	echo "##########trigger file for KSOP MODEL REPORT created ############################" >> $load_log

	echo "##########Extracting 3 report inputs files ############################" >> $load_log
	$shellScriptDir/mpw_drm_ims_create_table.sh &>>  $load_log
	echo "Done creating drm table" 2>&1 >>  $load_log
	$shellScriptDir/mpw_drm_ims_extract.sh &>>  $load_log
	echo "Done extracting drm files" 2>&1 >>  $load_log
	$shellScriptDir/mpw_fcs_accuracy_extract.sh &>>  $load_log
	echo "Done extracting fcs accuracy file" 2>&1 >>  $load_log
	$shellScriptDir/mpw_ims_risk_extract.sh &>>  $load_log
	echo "Done extracting ims risk file" 2>&1 >>  $load_log
	echo "##########End of Extracting 3 report inputs files ############################" >> $load_log
fi


echo "############ Worker Process end time $(date +'%d%m%Y%H%M%S') ############ "