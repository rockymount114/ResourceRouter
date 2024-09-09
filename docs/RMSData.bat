/*  
  Author:			Shotspotter Mandana Jamei
  Created Date:			01 OCT 2019
  Description:			Run Mission Extract SQL Script
*/ 

sqlcmd -S <sqlservername> -d <databasename> -E -W -h -1 -i "C:\ShotSpotter\filename.sql" -s "," -o "C:\ShotSpotter\DailyUpload_Data.csv"