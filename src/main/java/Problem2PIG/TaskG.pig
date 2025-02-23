friends = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/friends.csv' USING PigStorage(',') AS (friendrel, personid, myfriend, dateoffriendship, description);
pages = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/pages.csv' USING PigStorage(',') AS (id, name, nationality, countrycode, hobby);
access = LOAD 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/input/access_logs.csv' USING PigStorage(',') AS (accessid, bywho, whatpage, typeofaccess, accesstime:chararray);
grouped = GROUP access BY bywho;

userAccess = COGROUP pages BY id, access BY bywho;

userLatestAccess = FOREACH userAccess GENERATE
    FLATTEN(pages) AS (id:chararray, name:chararray, nationality:chararray, countrycode:chararray, hobby:chararray), ToDate((chararray)MAX(access.accesstime), 'yyyy-MM-dd HH:mm:ss') AS LatestAccessTime:datetime;

disconnectedPeople = FILTER userLatestAccess BY (LatestAccessTime IS NULL OR ABS(DaysBetween(LatestAccessTime, CurrentTime())) > 14);

final = FOREACH disconnectedPeople GENERATE id AS PersonID, name AS Name;

STORE final INTO 'C:/Users/andre/WPI/senior/CTerm/bigdata/Project2BigData/outputProblem2/outputG.csv' USING PigStorage(',');
