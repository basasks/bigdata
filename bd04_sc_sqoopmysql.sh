#!/bin/bash

###############################################################################################
# SQOOP-MYSQL COMMANDS
# Author: BASASKS
# Source: csv file (https://www.kaggle.com/skateddu/metacritic-all-time-games-stats)
# Environment: HDP2.6.5, Sqoop version: 1.4.6.2.6.5.0-292
# Usage: sh bd04_sc_sqoopmysql.sh
###############################################################################################



# COPY LOCAL FILE TO HDFS

sed -n '2,20001p' /home/maria_dev/proj/metacritic_games.csv > /home/maria_dev/proj/games
hdfs dfs -rm /user/maria_dev/proj04/games
hdfs dfs -copyFromLocal /home/maria_dev/proj/games /user/maria_dev/proj04/games


# PREPARE MYSQL TABLE PRE-EXPORT

# pre-running: `mysql_config_editor set --login-path=local --host=localhost --user=root --password1
mysql --login-path=local -e "DROP DATABASE IF EXISTS metacritic;"
mysql --login-path=local -e "CREATE DATABASE metacritic CHARACTER SET utf8;"
mysql --login-path=local -e "DROP TABLE IF EXISTS metacritic.games;"
mysql --login-path=local -e "CREATE TABLE metacritic.games ( name VARCHAR(255), platform VARCHAR(10), developer VARCHAR(255), publisher VARCHAR(255), genres VARCHAR(50), players VARCHAR(50), rating VARCHAR(10), attribute VARCHAR(100), release_date VARCHAR(20), link VARCHAR(255), critic_positive INTEGER, critic_neutral INTEGER, critic_negative INTEGER, metascore INTEGER, user_positive INTEGER, user_neutral INTEGER, user_negative INTEGER, user_score VARCHAR(10) )"
mysql --login-path=local -e "GRANT ALL PRIVILEGES ON metacritic.* to 'root'@'localhost';"


# EXPORT HDFS FILE TO MYSQL TABLE

sqoop export \
--connect jdbc:mysql://localhost/metacritic \
--username root \
--password-file /user/maria_dev/mysqlpwd.cfg \
--m 1 \
--table games \
--export-dir /user/maria_dev/proj04/games \
--input-lines-terminated-by '\n' \
--input-fields-terminated-by ',' \
--input-optionally-enclosed-by '\"' 


# PREPARE MYSQL TABLE PRE-IMPORT

mysql --login-path=local -e "DROP TABLE IF EXISTS metacritic.games_withid_mirror;"
mysql --login-path=local -e "CREATE TABLE metacritic.games_withid_mirror ( id INTEGER AUTO_INCREMENT, name VARCHAR(255), platform VARCHAR(10), developer VARCHAR(255), publisher VARCHAR(255), genres VARCHAR(50), players VARCHAR(50), rating VARCHAR(10), attribute VARCHAR(100), release_date VARCHAR(20), link VARCHAR(255), critic_positive INTEGER, critic_neutral INTEGER, critic_negative INTEGER, metascore INTEGER, user_positive INTEGER, user_neutral INTEGER, user_negative INTEGER, user_score VARCHAR(10), PRIMARY KEY (id) )"
mysql --login-path=local -e "INSERT INTO metacritic.games_withid_mirror (name, platform, developer, publisher, genres, players, rating, attribute, release_date, link, critic_positive, critic_neutral, critic_negative, metascore, user_positive, user_neutral, user_negative, user_score) SELECT * FROM metacritic.games"
mysql --login-path=local -e "DROP TABLE IF EXISTS metacritic.games_withid;"
mysql --login-path=local -e "CREATE TABLE metacritic.games_withid ( id INTEGER, name VARCHAR(255), platform VARCHAR(10), developer VARCHAR(255), publisher VARCHAR(255), genres VARCHAR(50), players VARCHAR(50), rating VARCHAR(10), attribute VARCHAR(100), release_date VARCHAR(20), link VARCHAR(255), critic_positive INTEGER, critic_neutral INTEGER, critic_negative INTEGER, metascore INTEGER, user_positive INTEGER, user_neutral INTEGER, user_negative INTEGER, user_score VARCHAR(10), PRIMARY KEY (id) )"
mysql --login-path=local -e "GRANT ALL PRIVILEGES ON metacritic.* to 'root'@'localhost';"


# IMPORT MYSQL TABLE TO HDFS FILE

hdfs dfs -rm -r /user/maria_dev/proj04/fromsqoop/
for i in 0 5000 10000 15000
do
	mysql --login-path=local -e "INSERT INTO metacritic.games_withid (id, name, platform, developer, publisher, genres, players, rating, attribute, release_date, link, critic_positive, critic_neutral, critic_negative, metascore, user_positive, user_neutral, user_negative, user_score) SELECT id, name, platform, developer, publisher, genres, players, rating, attribute, release_date, link, critic_positive, critic_neutral, critic_negative, metascore, user_positive, user_neutral, user_negative, user_score FROM metacritic.games_withid_mirror where id > ${i} and id <= ${i} + 5000;"
	sqoop import \
	--connect jdbc:mysql://localhost/metacritic \
	--username root \
	--password-file /user/maria_dev/mysqlpwd.cfg \
	--m 1 \
	--table games_withid \
	--target-dir /user/maria_dev/proj04/fromsqoop/ \
	--incremental append \
	--check-column id \
	--last-value ${i}
done


# IMPORT MYSQL TABLE TO HIVE TABLE WITH INCREMENTAL VALUE

hdfs dfs -rm -r /user/maria_dev/games_withid_mirror/
hive -e "DROP TABLE IF EXISTS proj04.games"
sqoop import \
--connect jdbc:mysql://localhost/metacritic \
--username sqlimport \
--password-file /user/maria_dev/mysqlpwd.cfg \
--table games_withid_mirror \
--columns "name, platform, developer, publisher, genres" \
--where "user_score <> 'tbd'" \
--split-by id \
--hive-import \
--create-hive-table \
--hive-table proj04.games
