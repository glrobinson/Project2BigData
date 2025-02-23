raw = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/access_logs.csv' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, TypeOfAccess, AccessTime);

grouped = GROUP raw BY ByWho;

final = FOREACH grouped {
    p = FOREACH raw GENERATE WhatPage;
    numDistinct = DISTINCT p;
    GENERATE group AS ByWho,
        COUNT(raw) AS TOTALPAGES,
        COUNT(numDistinct) AS DISTINCTPAGES;
        };

STORE final INTO 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/outputProblem2/outputE.csv';