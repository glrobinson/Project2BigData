raw = LOAD 'shared_folder/access_logs.csv' USING PigStorage(',') AS (accessID, byWho, whatPage, typeOfAccess, accessTime);
pages = LOAD 'shared_folder/pages.csv' USING PigStorage(',') AS (personID, name, nationality, country, hobby);

access = GROUP raw BY whatPage;

page_counts = FOREACH access GENERATE group as ID, COUNT(raw.accessID) as count;

sort = ORDER page_counts BY count DESC ;

top_10 = LIMIT sort 10;

total = join top_10 by ID, pages by personID;

select = FOREACH total GENERATE personID, name, nationality;

STORE select INTO 'shared_folder/outputB.csv' USING PigStorage(',');
