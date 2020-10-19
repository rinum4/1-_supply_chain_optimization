# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 15:47:41 2020

@author: Ринат
"""
from pulp import *

import pyodbc 
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=ERP245_2;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
cursor.execute('create table #tt1(_Q_000_F_000RRef binary(16), _Q_000_F_001RRef binary(16), _Q_000_F_002 int, _Q_000_F_003 int, _Q_000_F_004 int)')

#sqlstr = 'sp_executesql N'+'\'' + '''insert into #tt1 SELECT 
#T1._Fld15321RRef,'''
               
cursor.execute('sp_executesql N' + '\''+ ''' insert into #tt1 SELECT
T1._Document572_IDRRef,
T1._Fld15321RRef,
CAST(SUM(T1._Fld15326) AS NUMERIC(21, 3)),
CAST(SUM(T1._Fld15329) AS NUMERIC(21, 2)),
MAX(T2._Fld5563)
FROM dbo._Document572_VT15318 T1
LEFT OUTER JOIN dbo._Reference215 T2
ON (T1._Fld15321RRef = T2._IDRRef) AND (T2._Fld1551 = @P1)
LEFT OUTER JOIN dbo._Document572 T3
ON (T1._Document572_IDRRef = T3._IDRRef) AND (T3._Fld1551 = @P2)
WHERE ((T1._Fld1551 = @P3)) AND ((T3._Date_Time >= @P4) AND (T3._Date_Time <= @P5) AND (T3._Fld15255RRef = @P6))
GROUP BY T1._Document572_IDRRef,
T1._Fld15321RRef\',N\'@P1 numeric(10),@P2 numeric(10),@P3 numeric(10),@P4 datetime2(3),@P5 datetime2(3),@P6 varbinary(16)\',0,0,0,\'4020-01-01 00:00:00\',\'4020-01-18 00:00:00\',0x950BD481D76EDCFE11EA37711486B1FD''')

cursor.execute('create table #tt2(_Q_000_F_000RRef binary(16), _Q_000_F_001 int, _Q_000_F_002 int)')

cursor.execute('sp_executesql N' + '\''+ ''' insert into #tt2 SELECT
T9._Fld4865RRef,
T1.Fld38059Balance_,
T1.Fld38064Balance_
FROM (SELECT
T2.Fld38055RRef AS Fld38055RRef,
CAST(SUM(T2.Fld38059Balance_) AS NUMERIC(33, 3)) AS Fld38059Balance_,
CAST(SUM(T2.Fld38064Balance_) AS NUMERIC(33, 2)) AS Fld38064Balance_
FROM (SELECT
T3._Fld38055RRef AS Fld38055RRef,
CAST(SUM(T3._Fld38059) AS NUMERIC(27, 3)) AS Fld38059Balance_,
CAST(SUM(T3._Fld38064) AS NUMERIC(27, 2)) AS Fld38064Balance_
FROM dbo._AccumRgT38089 T3
LEFT OUTER JOIN dbo._Reference179 T4
ON (T3._Fld38055RRef = T4._IDRRef) AND (T4._Fld1551 = @P1)
WHERE ((T3._Fld1551 = @P2)) AND (T3._Period = @P3 AND ((T3._Fld38058RRef = @P4) AND T4._Fld4865RRef IN
(SELECT
T5._Q_000_F_000RRef AS Q_001_F_000RRef
FROM #tt1 T5 WITH(NOLOCK))) AND (T3._Fld38059 <> @P5 OR T3._Fld38064 <> @P6) AND (T3._Fld38059 <> @P7 OR T3._Fld38064 <> @P8))
GROUP BY T3._Fld38055RRef
HAVING (CAST(SUM(T3._Fld38059) AS NUMERIC(27, 3))) <> 0.0 OR (CAST(SUM(T3._Fld38064) AS NUMERIC(27, 2))) <> 0.0
UNION ALL SELECT
T6._Fld38055RRef AS Fld38055RRef,
CAST(CAST(SUM(CASE WHEN T6._RecordKind = 0.0 THEN T6._Fld38059 ELSE -T6._Fld38059 END) AS NUMERIC(21, 3)) AS NUMERIC(27, 3)) AS Fld38059Balance_,
CAST(CAST(SUM(CASE WHEN T6._RecordKind = 0.0 THEN T6._Fld38064 ELSE -T6._Fld38064 END) AS NUMERIC(21, 2)) AS NUMERIC(27, 2)) AS Fld38064Balance_
FROM dbo._AccumRg38054 T6
LEFT OUTER JOIN dbo._Reference179 T7
ON (T6._Fld38055RRef = T7._IDRRef) AND (T7._Fld1551 = @P9)
WHERE ((T6._Fld1551 = @P10)) AND (T6._Period >= @P11 AND T6._Period < @P12 AND T6._Active = 0x01 AND ((T6._Fld38058RRef = @P13) AND T7._Fld4865RRef IN
(SELECT
T8._Q_000_F_000RRef AS Q_001_F_000RRef
FROM #tt1 T8 WITH(NOLOCK))))
GROUP BY T6._Fld38055RRef
HAVING (CAST(CAST(SUM(CASE WHEN T6._RecordKind = 0.0 THEN T6._Fld38059 ELSE -T6._Fld38059 END) AS NUMERIC(21, 3)) AS NUMERIC(27, 3))) <> 0.0 OR (CAST(CAST(SUM(CASE WHEN T6._RecordKind = 0.0 THEN T6._Fld38064 ELSE -T6._Fld38064 END) AS NUMERIC(21, 2)) AS NUMERIC(27, 2))) <> 0.0) T2
GROUP BY T2.Fld38055RRef
HAVING (CAST(SUM(T2.Fld38059Balance_) AS NUMERIC(33, 3))) <> 0.0 OR (CAST(SUM(T2.Fld38064Balance_) AS NUMERIC(33, 2))) <> 0.0) T1
LEFT OUTER JOIN dbo._Reference179 T9
ON (T1.Fld38055RRef = T9._IDRRef) AND (T9._Fld1551 = @P14)\',N\'@P1 numeric(10),@P2 numeric(10),@P3 datetime2(3),@P4 varbinary(16),@P5 numeric(10),@P6 numeric(10),@P7 numeric(10),@P8 numeric(10),@P9 numeric(10),@P10 numeric(10),@P11 datetime2(3),@P12 datetime2(3),@P13 varbinary(16),@P14 numeric(10)\',0,0,\'4019-07-01 00:00:00\',0x950BD481D76EDCFE11EA37711486B1FD,0,0,0,0,0,0,\'4019-07-01 00:00:00\',\'4020-01-18 00:00:00\',0x950BD481D76EDCFE11EA37711486B1FD,0''')

cursor.execute('exec sp_executesql N' + '\''+ '''SELECT
((T3._Number + @P1) + T4._Description),
T1._Q_000_F_002,
T1._Q_000_F_004,
CAST(((CAST(T1._Q_000_F_003 AS NUMERIC(27, 8)) / T1._Q_000_F_002) - ISNULL(CAST((CAST(T2._Q_000_F_002 AS NUMERIC(38, 8)) / T2._Q_000_F_001) AS NUMERIC(38, 8)),@P2)) AS NUMERIC(10, 0))
FROM #tt1 T1 WITH(NOLOCK)
LEFT OUTER JOIN #tt2 T2 WITH(NOLOCK)
ON (T2._Q_000_F_000RRef = T1._Q_000_F_001RRef)
LEFT OUTER JOIN dbo._Document572 T3
ON (T1._Q_000_F_000RRef = T3._IDRRef) AND (T3._Fld1551 = @P3)
LEFT OUTER JOIN dbo._Reference215 T4
ON (T1._Q_000_F_001RRef = T4._IDRRef) AND (T4._Fld1551 = @P4)\',N\'@P1 nvarchar(4000),@P2 numeric(10),@P3 numeric(10),@P4 numeric(10)\',N\'_\',0,0,0''')

import string
alph = string.ascii_uppercase
#cursor.execute('select * from #tt2')
prod = {}
weight = {}
quantity = {}
prof = {}
i=0
row = cursor.fetchone()
while row:
#    print (str(row[0]) + " " + str(row[1]) + " " + str(row[2]) + " " + str(row[3]))
#    print (str(row[0]) + " " + str(row[1]) + " " + str(row[2]))
    prod[i] = alph[i]
    quantity[i] = row[1]
    weight[i] = row[2]
    prof[i] = float(row[3])
    i=i+1
    row = cursor.fetchone()

cursor.execute('exec sp_executesql N' + '\''+ '''SELECT
T1._Description,
T1._Fld10375
FROM dbo._Reference431 T1
WHERE (T1._Fld1551 = @P1)\',N\'@P1 numeric(10)\',0''')

#run_pulp_model('ts1', quantity, 10000)
#run_pulp_model('ts2', quantity, 50000)
#run_pulp_model('ts3', quantity, 20000)
#run_pulp_model('ts4', quantity, 25000)

row = cursor.fetchone()
while row:
#    print (str(row[0]) + " " + str(row[1]) + " " + str(row[2]) + " " + str(row[3]))
#    print (str(row[0]) + " " + str(row[1]) + " " + str(row[2]))
#    print (str(row[0]) + " " + str(row[1]))
    run_pulp_model(str(row[0]), quantity, float(row[1])*1000)
    row = cursor.fetchone()

def run_pulp_model(ts, quantity, max_weight):  
    # Initialize Class 
    model = LpProblem("Loading Truck Problem", LpMaximize)
 
    # Define Decision Variables 
    x = LpVariable.dicts('ship_', prod, lowBound=0, cat='Integer')  
    
    # Define Objective 
    model += lpSum([prof[i]*x[i] for i in prod]) 
     
    # Define Constraint 
    model += lpSum([weight[i]*x[i] for i in prod]) <= max_weight
    
    # Define Constraint 
    for i in prod:
        model += x[i] <= quantity[i]
    
    # Solve Model 
    model.solve() 
    
    print("Transport {}".format(ts))
    for i in prod: 
        print("{} quantity {}".format(x[i], x[i].varValue))
        quantity[i]=quantity[i] - x[i].varValue
        
#print("Total Cost = ", value(model.objective))