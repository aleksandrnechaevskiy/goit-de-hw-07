USE olympic_dataset;

CREATE TABLE athlete_event_results (
  edition      VARCHAR(255),
  edition_id   VARCHAR(50),
  country_noc  VARCHAR(50),
  sport        VARCHAR(255),
  event        TEXT,
  result_id    VARCHAR(50),
  athlete      TEXT,
  athlete_id   VARCHAR(50),
  pos          VARCHAR(255),
  medal        VARCHAR(50),
  isTeamSport  VARCHAR(10)
) CHARACTER SET utf8mb4;

LOAD DATA INFILE '/var/lib/mysql-files/athlete_event_results.csv'
INTO TABLE athlete_event_results
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
