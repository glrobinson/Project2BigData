raw = LOAD 'shared_folder/pages.csv' USING PigStorage(',') AS (personID, name, nationality, country, hobby);

clean1 = FILTER raw BY nationality == 'Dominica';

select = FOREACH clean1 GENERATE name, hobby;

STORE select INTO 'shared_folder/outputA.csv' USING PigStorage(',');