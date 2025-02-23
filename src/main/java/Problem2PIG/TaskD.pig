pages = LOAD 'shared_folder/pages.csv' USING PigStorage(',') AS (personID, name, nationality, country, hobby);
friends = LOAD 'shared_folder/friends.csv' USING PigStorage(',') AS (friendRel, personID, myFriend, dateOfFriendship, descript);

group_friends = GROUP friends BY myFriend;

count_friends = FOREACH group_friends GENERATE group as ID, COUNT(friends) as count;

join_pages_friends = JOIN pages BY personID LEFT OUTER, count_friends BY ID;

select = FOREACH join_pages_friends GENERATE name, (count is null ? 0 : count );

STORE select INTO 'shared_folder/outputD.csv' USING PigStorage(',');
