query_txt = f'''SELECT 
u.name
, u.unitperid
, u.unitperid

, l.agency
, l.vehicleid
, u.officerid

, l.geox
, l.geoy
, l.latitude
, l.longitude

, l.timestamp

  FROM [Cad].[dbo].[avllog] l
  JOIN [dbo].[unitper]		u
  ON l.unitcode = u.unitcode
  AND l.unitperid = u.unitperid

  WHERE l.timestamp >= '2024-09-01'

  --and u.officerid = 3676

  ORDER BY u.officerid, l.timestamp
  '''