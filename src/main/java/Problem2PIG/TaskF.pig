friends = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/friends.csv' USING PigStorage(',') AS (friendrel, personid, myfriend, dateoffriendship, description);
pages = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/pages.csv' USING PigStorage(',') AS (id, name, nationality, countrycode, hobby);
access = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/access_logs.csv' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, TypeOfAccess, AccessTime);

friendswithaccess = JOIN friends BY (personid, myfriend) LEFT OUTER, access BY (ByWho, WhatPage);

noAccess = FILTER friendswithaccess BY access::ByWho IS NULL;

p1info = FOREACH noAccess GENERATE friends::personid AS p1;
p1withoutaccess = DISTINCT p1info;

result = JOIN p1withoutaccess BY p1, pages BY id;

final = FOREACH result GENERATE p1withoutaccess::p1 AS PersonID, pages::name AS Name;

STORE final INTO 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/outputProblem2/outputF.csv' USING PigStorage(',');