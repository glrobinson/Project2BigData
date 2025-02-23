friends = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/friends.csv' USING PigStorage(',') AS (friendrel, personid, myfriend, dateoffriendship, description);
pages = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/pages.csv' USING PigStorage(',') AS (id, name, nationality, countrycode, hobby);
access = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/access_logs.csv' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, TypeOfAccess, AccessTime);

groupfriends = GROUP friends BY myfriend;
friendscounted = FOREACH groupfriends GENERATE group AS id, COUNT(friends) AS count;

pagesfriends = JOIN pages BY id LEFT OUTER, friendscounted BY id;

pagessorted = FOREACH pagesfriends GENERATE pages::id AS id, pages::name AS name, ((friendscounted::count IS NOT NULL) ? friendscounted::count : 0) AS count;

pageswithkey = FOREACH pagessorted GENERATE id, name, count, 1 AS k;

grouped = GROUP pageswithkey BY k;

average = FOREACH grouped GENERATE AVG(pageswithkey.count) AS averagefriends, group AS k;

joined = JOIN pageswithkey BY k, average BY k;

popular = FILTER joined BY pageswithkey::count > average::averagefriends;

final = FOREACH popular GENERATE pageswithkey::id AS PersonID, pageswithkey::name AS Name, pageswithkey::count AS FriendCount;

STORE final INTO 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/outputProblem2/outputH.csv' USING PigStorage(',');