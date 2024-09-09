SET NOCOUNT ON 
/*  
  Author:				Shotspotter Mandana Jamei
  Created Date:			19/NOV/2019
  Description:			Mission Extract With date time Conversion from Local to UTC
*/ 

/* Created Time Table*/
IF OBJECT_ID('tempdb..#DST') IS NOT NULL
DROP TABLE #DST

IF OBJECT_ID('tempdb..#Tmp1') IS NOT NULL

DROP TABLE #Tmp1

CREATE TABLE #DST
       (
             Year		INT
          ,  StartDate	DATETIME
          ,  EndDate	DATETIME
       )

INSERT INTO #DST
       (Year, StartDate, EndDate)
       VALUES
             (2020,'2020-03-08 02:00:00', '2020-11-01 02:00:00'),
             (2019,'2019-03-11 02:00:00', '2019-11-03 02:00:00'),
             (2018,'2018-03-12 02:00:00', '2018-11-04 02:00:00'),
             (2017,'2017-03-13 02:00:00', '2017-11-05 02:00:00'),
             (2016,'2016-03-08 02:00:00', '2016-11-06 02:00:00'),
             (2015,'2015-03-09 02:00:00', '2015-11-01 02:00:00'),
             (2014,'2014-03-10 02:00:00', '2014-11-02 02:00:00'),
             (2013,'2013-03-11 02:00:00', '2013-11-03 02:00:00'),
             (2012,'2012-03-13 02:00:00', '2012-11-04 02:00:00')

/* Get the Season*/

DECLARE @SummerDLS INT -- Summer Day light Saving
DECLARE @WinterDLS INT

DECLARE @localtime		DATETIME;
DECLARE @gmttime		DATETIME;
DECLARE @UTCDiff		FLOAT;

SET		@localtime	=	GETDATE();
SET		@gmttime	=	GETUTCDATE();
SET     @UTCDiff    =   DATEDIFF(HOUR,@gmttime,@localtime);

--select @localtime,@gmttime, @UTCDiff  -- testing

DECLARE @StartSummer		DATETIME;
DECLARE @EndSummer		    DATETIME;
SELECT @StartSummer = StartDate, @EndSummer = EndDate FROM #DST WHERE [Year] = DATEPART(YEAR,GETDATE())
-- select  @StartSummer StartSuner ,  @EndSummer EndSummer  -- testing

IF @localtime BETWEEN @StartSummer AND @EndSummer 
	BEGIN
		SET @SummerDLS = @UTCDiff 
		SET @WinterDLS = @UTCDiff -1 
	END
ELSE
	BEGIN
		SET @SummerDLS = @UTCDiff +1
		SET @WinterDLS = @UTCDiff 
	END 
---select  @SummerDLS SummerDSL, @WinterDLS WinterDSL  -- testing

/* Get the data*/

SELECT
			lwmain.inci_id														AS id
		,	CAST(REPLACE(lwmain.streetnbr,',','') AS CHAR(8))+ lwmain.street	AS [address]
		,	CASE WHEN  LEN(lwmain.geox) < 9 
				 THEN CAST(((CAST(lwmain.geox AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geox)))))/100) AS DECIMAL (16,2)) 
				 ELSE CAST((CAST(lwmain.geox AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) 
				 END															AS pointx
		,	CASE WHEN  LEN(lwmain.geoy) < 9 
				 THEN CAST(((CAST(lwmain.geoy AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geoy)))))/100) AS DECIMAL (16,2)) 
				 ELSE CAST((CAST(lwmain.geoy AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) 
				 END															AS pointy
		,	lwchrg.ibr_code														AS class
		,   lwmain.date_occu													-- datetimefrom
		,   lwmain.date_fnd														-- datetimeto
		,   lwmain.date_rept													-- report_time
		,   lwmain.reviewdate													-- last_updated
        ,	CASE
             WHEN lwmain.date_occu BETWEEN dst.StartDate AND dst.EndDate
             THEN @SummerDLS
             ELSE @WinterDLS
             END																AS UTCOffset

INTO #Tmp1
FROM		lwmain     
INNER JOIN	lwchrg  
	ON lwmain.lwmainid = lwchrg.lwmainid
INNER JOIN #DST dst
    ON DATEPART(yy, lwmain.date_occu) = dst.Year
WHERE	lwmain.date_occu  BETWEEN DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0) AND GETDATE()
AND lwmain.geox != 0
AND lwmain.geoy != 0


SELECT  'id','address','pointx','pointy','class', 'datetimefrom','datetimeto','report_time','last_updated','datasource'  --The header
SELECT
       id,
       [address],
       pointx,
       pointy,
       class,
       CONVERT(varchar, CONVERT(DATETIMEOFFSET(0), DATEADD(hh, -UTCOffset, date_occu)), 126)							AS datetimefrom,
       CONVERT(varchar, CONVERT(DATETIMEOFFSET(0), DATEADD(hh, -UTCOffset, date_fnd )), 126)							AS datetimeto,

	   CASE WHEN (date_rept IS NULL OR date_rept =' ') THEN CONVERT(varchar, CONVERT(DATETIMEOFFSET(0), DATEADD(hh, -UTCOffset, date_fnd)), 126)
						ELSE CONVERT(varchar, CONVERT(DATETIMEOFFSET(0), DATEADD(hh, -UTCOffset, date_rept)), 126) END	AS report_time,

	   CASE WHEN (reviewdate IS NULL OR reviewdate =' ') THEN CONVERT(varchar, CONVERT(DATETIMEOFFSET(0), GETUTCDATE()), 126)
					ELSE CONVERT(varchar, CONVERT(DATETIMEOFFSET(0), DATEADD(hh, -UTCOffset, reviewdate)), 126) END		AS last_updated,


       'GIS_CRIME'  AS datasource
FROM #Tmp1
ORDER BY datetimefrom

