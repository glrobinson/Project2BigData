raw = LOAD 'shared_folder/pages.csv' USING PigStorage(',') AS (personID, name, nationality, country, hobby);

country_groups = GROUP raw BY (nationality);

country_group_counts = FOREACH country_groups GENERATE group, COUNT(raw);

STORE country_group_counts INTO 'shared_folder/outputC.csv' USING PigStorage(',');