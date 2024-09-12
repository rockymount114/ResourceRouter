
initial_query = f'''
SELECT
        lwmain.inci_id AS id,
        CAST(REPLACE(lwmain.streetnbr,',','') AS CHAR(8)) + lwmain.street AS [address],
        CASE WHEN LEN(lwmain.geox) < 9 THEN CAST(((CAST(lwmain.geox AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geox)))))/100) AS DECIMAL(16,2)) 
            ELSE CAST((CAST(lwmain.geox AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) END AS pointx,
        CASE WHEN LEN(lwmain.geoy) < 9 THEN CAST(((CAST(lwmain.geoy AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geoy)))))/1000) AS DECIMAL(16,2)) 
            ELSE CAST((CAST(lwmain.geoy AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) END AS pointy,       

		lwchrg.ibr_code AS class,
        FORMAT(lwmain.date_occu, 'yyyy-MM-ddTHH:mm:ss+00:00') AS datetimefrom,
        FORMAT(lwmain.date_fnd, 'yyyy-MM-ddTHH:mm:ss+00:00') AS datetimeto,
        FORMAT(lwmain.date_rept, 'yyyy-MM-ddTHH:mm:ss+00:00') AS report_time,
        FORMAT(lwmain.date_rept, 'yyyy-MM-ddTHH:mm:ss+00:00') AS last_updated,
        'rms8' AS datasource
    FROM lwmain     
    JOIN lwchrg ON lwmain.lwmainid = lwchrg.lwmainid
    WHERE lwmain.inci_id IS NOT NULL
    AND lwmain.geox != 0
    AND lwmain.geoy != 0
    AND lwmain.date_rept >= '2019-01-01'

'''


# this for daily data within one month data
daily_query = f'''
    SELECT 
        lwmain.inci_id AS id,
        CAST(REPLACE(lwmain.streetnbr,',','') AS CHAR(8)) + lwmain.street AS [address],
        
        CASE WHEN LEN(lwmain.geox) < 9 THEN CAST(((CAST(lwmain.geox AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geox)))))/100) AS DECIMAL(16,2)) 
            ELSE CAST((CAST(lwmain.geox AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) END AS pointx,
        CASE WHEN LEN(lwmain.geoy) < 9 THEN CAST(((CAST(lwmain.geoy AS DECIMAL(16,2)) * (POWER(10, (9 - LEN(lwmain.geoy)))))/1000) AS DECIMAL(16,2)) 
            ELSE CAST((CAST(lwmain.geoy AS DECIMAL(16,2)))/100 AS DECIMAL(16,2)) END AS pointy,
        
		lwmain.city,
		lwmain.state,
		lwmain.latitude,
		lwmain.longitude,

		lwchrg.ibr_code AS class,
        FORMAT(lwmain.date_occu, 'yyyy-MM-ddTHH:mm:ss+00:00') AS datetimefrom,
        FORMAT(lwmain.date_fnd, 'yyyy-MM-ddTHH:mm:ss+00:00') AS datetimeto,
        FORMAT(lwmain.date_rept, 'yyyy-MM-ddTHH:mm:ss+00:00') AS report_time,
        FORMAT(lwmain.date_rept, 'yyyy-MM-ddTHH:mm:ss+00:00') AS last_updated,
        'rms8' AS datasource
    FROM lwmain     
    JOIN lwchrg ON lwmain.lwmainid = lwchrg.lwmainid
    WHERE lwmain.inci_id IS NOT NULL
    AND lwmain.geox != 0
    AND lwmain.geoy != 0
    AND lwmain.date_rept >= DATEADD(day, -30, GETDATE())       
'''

# this avl query has already corrected the x,y coordinates, csv size is 500MB
avl_query = f'''
                    SELECT 
                    u.name
                    , u.unitperid
                    --, l.inci_id
                    --, l.status
                    --, l.speed
                    --, l.heading
                    --, l.agency
                    --, l.vehicleid
                    --, u.officerid
                    --, u.emdept_id
                    --, u.intime
                    --, u.outtime
                    , l.geox
                    , l.geoy
                    , l.latitude
                    , l.longitude
                    , l.timestamp
                    FROM [Cad].[dbo].[avllog] l
                    JOIN [dbo].[unitper] u
                    ON l.unitcode = u.unitcode
                    AND l.unitperid = u.unitperid

                    WHERE l.timestamp >= DATEADD(hour, -4, GETDATE())
  
'''


# daily_query = f'''
# SELECT [id]
#       ,[address]
#       ,[pointx]
#       ,[pointy]
#       ,[class]
#       ,[datetimefrom]
#       ,[datetimeto]
#       ,[report_time]
#       ,[last_updated]
#       ,[datasource]
#   FROM [dbo].[v_inci_api]
#   WHERE [datetimefrom] >= DATEADD(DAY, -1, GETDATE())

# '''
# initial_query = f'''
# SELECT [id]
#       ,[address]
#       ,[pointx]
#       ,[pointy]
#       ,[class]
#       ,[datetimefrom]
#       ,[datetimeto]
#       ,[report_time]
#       ,[last_updated]
#       ,[datasource]
#   FROM [dbo].[v_inci_api]

# '''