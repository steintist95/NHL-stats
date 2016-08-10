import urllib2
import mysql.connector
from mysql.connector import MySQLConnection, Error, errorcode
from bs4 import BeautifulSoup as bs
import logging
import sys
import json
import datetime
#experiment with pyquery for choice of table
logging.basicConfig(filename='statsLog.log', level=logging.DEBUG,format='%(asctime)s %(levelname)s: %(message)s ', datefmt="%m-%d-%Y %H:%M:%S")
logging.info('\t\t\t\tSTARTING NHL SCRAPE')
teamDict = {
    'Canadiens': 'Montreal Canadiens',
    'Jets': 'Winnipeg Jets',
    'Stars': 'Dallas Stars',
    'Rangers': 'New York Rangers',
    'Blues': 'St. Louis Blues',
    'Islanders': 'New York Islanders',
    'Capitals': 'Washington Capitals',
    'Wild': 'Minnesota Wild',
    'Predators': 'Nashville Predators',
    'Kings': 'Los Angeles Kings',
    'Canucks': 'Vancouver Canucks',
    'Penguins': 'Pittsburgh Penguins',
    'Blackhawks': 'Chicago Blackhawks',
    'Senators': 'Ottawa Senators',
    'Lightning': 'Tampa Bay Lightning',
    'Red Wings': 'Detroit Red Wings',
    'Devils': 'New Jersey Devils',
    'Panthers': 'Florida Panthers',
    'Sharks': 'San Jose Sharks',
    'Coyotes': 'Arizona Coyotes',
    'Hurricanes': 'Carolina Hurricanes',
    'Sabres': 'Buffalo Sabres',
    'Flyers': 'Philadelphia Flyers',
    'Oilers': 'Edmonton Oilers',
    'Avalanche': 'Colorado Avalanche',
    'Flames': 'Calgary Flames',
    'Maple Leafs': 'Toronto Maple Leafs',
    'Ducks': 'Anaheim Ducks',
    'Blue Jackets': 'Columbus Blue Jackets',
    'Bruins': 'Boston Bruins'}
    


goalie_table_sql = (
        "CREATE TABLE `Goalies`" 
        "(`Goalie_Number`              int             NOT NULL,"
        "`Goalie_Name`                 varchar(50)     NOT NULL,"
        "`Team`                        varchar(50)     NOT NULL,"
        "`Games_Played_In`             int             NOT NULL,"
        "`Games_Started`               int             NOT NULL,"
        "`Minutes`                     int             NOT NULL,"
        "`Goals_Against_Average`       double(5,2)     NOT NULL,"
        "`Wins`                        int             NOT NULL,"
        "`Losses`                      int             NOT NULL,"
        "`Overtime_Losses`             int             NOT NULL,"
        "`Shutouts`                    int             NOT NULL,"
        "`Shots_Against`               int             NOT NULL,"
        "`Goals_Against`               int             NOT NULL,"
        "`Save_Percentage`             double(4,3)     NOT NULL,"
        "`Goalie_Goals`                int             NOT NULL,"
        "`Goalie_Assists`              int             NOT NULL,"
        "`Goalie_Penalty_Minutes`      int             NOT NULL,"
        "PRIMARY KEY(`Goalie_Name`)"
        ") ENGINE=InnoDB;"
        )
skater_table_sql = (
    "CREATE TABLE %s"
    "(`Skater_Number`              int             NOT NULL,"
    "`Position`                    varchar(10)     NOT NULL,"
    "`Skater_Name`                 varchar(60)     NOT NULL,"
    "`Games_Played`                int             NOT NULL,"
    "`Skater_Goals`                int             NOT NULL,"
    "`Skater_Assists`              int             NOT NULL,"
    "`Points`                      int             NOT NULL,"
    "`Plus_Minus`                  int             NOT NULL,"
    "`Skater_Penalty_Minutes`      int             NOT NULL,"
    "`Power_Play_Goals`            int             NOT NULL,"
    "`Short_Handed_Goals`          int             NOT NULL,"
    "`Game_Winning_Goals`          int             NOT NULL,"
    "`Shots`                       int             NOT NULL,"
    "`Shot_Percentage`             double(4,1)     NOT NULL,"
    "PRIMARY KEY(`Skater_Name`)"
    ") ENGINE=InnoDB;"
)
year = 2016
# for team, value in teamDict.iteritems():
#     official_team = teamDict[team]
with open('data.json', 'r') as data:
    loader = json.load(data)
    user = loader['user']
    host = loader['host']
    password = loader['password']
    database = loader['database']
 
special_url = ['http://predators.nhl.com/club/stats.htm?gameType=2&season=20152016']

r = ['http://rangers.nhl.com/club/stats.htm?gameType=2&season=20152016&srt=pnum']#'rangers':'http://rangers.nhl.com/club/stats.htm?gameType=2&season=20152016'} #first one is sorted by number

urls_skater0_goalie1 = ['http://senators.nhl.com/club/stats.htm',
'http://blackhawks.nhl.com/club/stats.htm?gameType=2&season=20152016 ','http://hurricanes.nhl.com/club/stats.htm', 'http://ducks.nhl.com/club/stats.htm?gameType=2&season=20152016', 'http://mapleleafs.nhl.com/club/stats.htm', 'http://avalanche.nhl.com/club/stats.htm', 'http://canadiens.nhl.com/club/stats.htm?gameType=3&season=20152016', 'http://bruins.nhl.com/club/stats.htm', 'http://sharks.nhl.com/club/stats.htm?gameType=2&season=20152016','http://sabres.nhl.com/club/stats.htm','http://coyotes.nhl.com/club/stats.htm', 'http://flames.nhl.com/club/stats.htm','http://stars.nhl.com/club/stats.htm?gameType=2&season=20152016', 'http://wild.nhl.com/club/stats.htm?gameType=2&season=20152016','http://canucks.nhl.com/club/stats.htm','http://penguins.nhl.com/club/stats.htm?gameType=2&season=20152016', 'http://lightning.nhl.com/club/stats.htm?gameType=2&season=20152016','http://capitals.nhl.com/club/stats.htm?gameType=2&season=20152016','http://flyers.nhl.com/club/stats.htm?gameType=2&season=20152016']

#predators belongs in 0_1
#rangers belongs in 1_2
urls_skater1_goalie2 = ['http://devils.nhl.com/club/stats.htm?gameType=2&season=20152016',  'http://panthers.nhl.com/club/stats.htm?gameType=2&season=20152016','http://redwings.nhl.com/club/stats.htm?gameType=2&season=20152016', 'http://blues.nhl.com/club/stats.htm?gameType=2&season=20152016','http://jets.nhl.com/club/stats.htm','http://bluejackets.nhl.com/club/stats.htm','http://islanders.nhl.com/club/stats.htm?gameType=2&season=20152016']#GOOD

# for row in soup('table')[4]/[5].findAll('tr'): #oilers good
# for row in soup('table')[2]/[3].findAll('tr'): # kings good
# for row in soup('table')[1]/[2].findAll('tr'):  #devils good
#for row in soup('table')[1]/[2].findAll('tr'): #rangers good
#for row in soup('table')[1]/[2].findAll('tr'): #panthers good
# for row in soup('table')[1]/[2].findAll('tr'): #red wings good
#for row in soup('table')[1]/[2].findAll('tr'): #blues  
#soup must change based on team
 
urls_skater4_goalie5 = ['http://oilers.nhl.com/club/stats.htm?gameType=2&season=20152016'] #GOOD
urls_skater2_goalie3 = ['http://kings.nhl.com/club/stats.htm?gameType=2&season=20152016'] #GOOD

#note for predators: use for row in soup('table')[0].findAll('tr')[1:-1] and manually add last one
#note for rangers:  use for row in soup('table')[1].findAll('tr')[1:-4] skaters when sorted by number, ...('table')[2].findAll('tr')[1:-1] goalies enter manually later
conn = MySQLConnection(host=host, user=user, password=password, database=database)
cursor = conn.cursor()

if conn.is_connected():
    print 'Connected'
    logging.info('CONNECTED TO DATABASE')
    cursor.execute("USE sports;")

    try:
        logging.info('\tCREATING SPORTS DATABASE')
        cursor.execute("CREATE DATABASE IF NOT EXISTS `sports`;")
    except Error as err:
        logging.error(err.upper())
    else:
        logging.info('\t\tSPORTS DATABASE CREATED')

    try:
        logging.info('CREATING GOALIE TABLE')
        cursor.execute(goalie_table_sql)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            logging.error('\tGOALIE TABLE ALREADY EXISTS')
        else:
            logging.error(err.upper())
    else:
        logging.info('\tGOALIE TABLE CREATED')

    for item in urls_skater0_goalie1:
        page	= urllib2.urlopen(item).read()
        soup	=bs(page,  'lxml')
        team = item.split('http://')[1].split('.')[0]

        try:
            print 'CREATING {} TABLE'.format(team)
            logging.info('CREATING {} TABLE TABLE'.format(team.upper()))
            cursor.execute(skater_table_sql % team)
        except Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error('\t{} TABLE EXISTS'.format(team.upper()))
            else:
                logging.error('\t'+err.msg.upper())
        else:
            logging.info('\t{} TEAM TABLE CREATED'.format(team.upper()))

        for row in soup('table')[0].findAll('tr')[1:]:
            stats = row.text.strip().encode('ascii').split('\n')

            skater_number = stats[0]
            position = stats[1]
            skater_name = stats[2]
            games_played = stats[3]
            goals = stats[4]
            assists = stats[5]
            points = stats[6]
            plus_minus = stats[7]
            penalty_minutes = stats[8]
            power_play_goals = stats[9]
            short_handed_goals = stats[10]
            game_winning_goals = stats[11]
            shots = stats[12]
            shot_percentage = stats[13]

            data_skater = {
                'skater_number':int(skater_number),
                'position':str(position),
                'skater_name':str(skater_name),
                'games_played':int(games_played),
                'goals':int(goals),
                'assists':int(assists),
                'points':int(points),
                'plus_minus':int(plus_minus),
                'penalty_minutes':int(penalty_minutes),
                'power_play_goals':int(power_play_goals),
                'short_handed_goals':int(short_handed_goals),
                'game_winning_goals':int(game_winning_goals),
                'shots':int(shots),
                'shot_percentage':float(shot_percentage),}

            add_skater_stats = (
                'INSERT INTO %s '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                ) % (team, skater_number,position,skater_name,games_played,goals,assists,points,plus_minus,penalty_minutes,power_play_goals,short_handed_goals,game_winning_goals,shots,shot_percentage)

            try:
                logging.info("\tINSERTING {}'S PLAYER STATS INTO {} TEAM TABLE".format(skater_name.upper(), team.upper()))
                cursor.execute(add_skater_stats, data_skater)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tMYSQL ERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(skater_name.upper()))

        for row in soup('table')[1].findAll('tr')[1:]:
            stats = row.text.strip().encode('ascii').split('\n')

            goalie_number=stats[0]
            goalie_name=stats[1]
            games_played_in=stats[2]
            games_started=stats[3]
            minutes=stats[4]
            goals_against_average=stats[5]
            wins=stats[6]
            losses=stats[7]
            overtime_losses=stats[8]
            shutouts=stats[9]
            shots_against=stats[10]
            goals_against=stats[11]
            save_percentage=stats[12]
            goalie_goals=stats[13]
            goalie_assists=stats[14]
            goalie_penalty_minutes=stats[15]

            data_goalie = {
                'Goalie_Number':int(goalie_number),
                'Goalie_Name':str(goalie_name),
                'Team':str(team.upper()[0]),
                'Games_Played_In':int(games_played_in),
                'Games_Started':int(games_started),
                'Minutes':int(minutes),
                'Goals_Against_Average':float(goals_against_average),
                'Wins':int(wins),
                'Losses':int(losses),
                'Overtime_Losses':int(overtime_losses),
                'Shutouts':int(shutouts),
                'Shots_Against':int(shots_against),
                'Goals_Against':int(goals_against),
                'Save_Percentage':float(save_percentage),
                'Goalie_Goals':int(goalie_goals),
                'Goalie_Assists':int(goalie_assists),
                'Goalie_Penalty_Minutes':int(goalie_penalty_minutes),}

            add_goalie_stats = (
                'INSERT INTO `GOALIES` '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ) % (goalie_number,goalie_name,team,games_played_in,games_started,minutes,goals_against_average,wins,losses,overtime_losses,shutouts,shots_against,goals_against,save_percentage,goalie_goals,goalie_assists,goalie_penalty_minutes)

            try:
                logging.info("\tINSERTING {}'S GOALIE STATS INTO GOALIE TABLE".format(goalie_name.upper()))
                cursor.execute(add_goalie_stats, data_goalie)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(goalie_name.upper()))

    for item in r:
        page = urllib2.urlopen(item).read()
        soup = bs(page, 'lxml')
        team = item.split('http://')[1].split('.')[0]

        try:
            print 'CREATING {} TABLE'.format(team)
            logging.info('CREATING {} TABLE TABLE'.format(team.upper()))
            cursor.execute(skater_table_sql % team)
        except Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error('\t{} TABLE EXISTS'.format(team.upper()))
            else:
                logging.error('\t' + err.msg.upper())
        else:
            logging.info('\t{} TEAM TABLE CREATED'.format(team.upper()))

        for row in soup('table')[1].findAll('tr')[1:-3]:
            stats = row.text.strip().encode('ascii').split('\n')

            skater_number = stats[0]
            position = stats[1]
            skater_name = stats[2]
            games_played = stats[3]
            goals = stats[4]
            assists = stats[5]
            points = stats[6]
            plus_minus = stats[7]
            penalty_minutes = stats[8]
            power_play_goals = stats[9]
            short_handed_goals = stats[10]
            game_winning_goals = stats[11]
            shots = stats[12]
            shot_percentage = stats[13]

            data_skater = {
                'skater_number': int(skater_number),
                'position': str(position),
                'skater_name': str(skater_name),
                'games_played': int(games_played),
                'goals': int(goals),
                'assists': int(assists),
                'points': int(points),
                'plus_minus': int(plus_minus),
                'penalty_minutes': int(penalty_minutes),
                'power_play_goals': int(power_play_goals),
                'short_handed_goals': int(short_handed_goals),
                'game_winning_goals': int(game_winning_goals),
                'shots': int(shots),
                'shot_percentage': float(shot_percentage),}

            add_skater_stats = (
                'INSERT INTO %s '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                ) % (team, skater_number, position, skater_name, games_played, goals, assists, points,
                plus_minus, penalty_minutes, power_play_goals, short_handed_goals,
                game_winning_goals, shots, shot_percentage)

            try:
                logging.info("\tINSERTING {}'S PLAYER STATS INTO {} TEAM TABLE".format(skater_name.upper(), team.upper()))
                cursor.execute(add_skater_stats, data_skater)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tMYSQL ERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(skater_name.upper()))

        for row in soup('table')[2].findAll('tr')[1:-1]:
            stats = row.text.strip().encode('ascii').split('\n')

            goalie_number = stats[0]
            goalie_name = stats[1]
            games_played_in = stats[2]
            games_started = stats[3]
            minutes = stats[4]
            goals_against_average = stats[5]
            wins = stats[6]
            losses = stats[7]
            overtime_losses = stats[8]
            shutouts = stats[9]
            shots_against = stats[10]
            goals_against = stats[11]
            save_percentage = stats[12]
            goalie_goals = stats[13]
            goalie_assists = stats[14]
            goalie_penalty_minutes = stats[15]

            data_goalie = {
                'Goalie_Number': int(goalie_number),
                'Goalie_Name': str(goalie_name),
                'Team': str(team.upper()[0]),
                'Games_Played_In': int(games_played_in),
                'Games_Started': int(games_started),
                'Minutes': int(minutes),
                'Goals_Against_Average': float(goals_against_average),
                'Wins': int(wins),
                'Losses': int(losses),
                'Overtime_Losses': int(overtime_losses),
                'Shutouts': int(shutouts),
                'Shots_Against': int(shots_against),
                'Goals_Against': int(goals_against),
                'Save_Percentage': float(save_percentage),
                'Goalie_Goals': int(goalie_goals),
                'Goalie_Assists': int(goalie_assists),
                'Goalie_Penalty_Minutes': int(goalie_penalty_minutes),}

            add_goalie_stats = (
                'INSERT INTO `GOALIES` '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ) % (goalie_number, goalie_name, team, games_played_in, games_started, minutes,goals_against_average, wins, losses, overtime_losses, shutouts, shots_against,goals_against, save_percentage, goalie_goals, goalie_assists,goalie_penalty_minutes)

            try:
                logging.info("\tINSERTING {}'S GOALIE STATS INTO GOALIE TABLE".format(goalie_name.upper()))
                cursor.execute(add_goalie_stats, data_goalie)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(goalie_name.upper()))

    for item in urls_skater1_goalie2:
        page = urllib2.urlopen(item).read()
        soup = bs(page, 'lxml')
        team = item.split('http://')[1].split('.')[0]

        try:
            print 'CREATING {} TABLE'.format(team)
            logging.info('CREATING {} TABLE TABLE'.format(team.upper()))
            cursor.execute(skater_table_sql % team)
        except Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error('\t{} TABLE EXISTS'.format(team.upper()))
            else:
                logging.error('\t' + err.msg.upper())
        else:
            logging.info('\t{} TEAM TABLE CREATED'.format(team.upper()))

        for row in soup('table')[0].findAll('tr')[1:]:
            stats = row.text.strip().encode('ascii').split('\n')

            skater_number = stats[0]
            position = stats[1]
            skater_name = stats[2]
            games_played = stats[3]
            goals = stats[4]
            assists = stats[5]
            points = stats[6]
            plus_minus = stats[7]
            penalty_minutes = stats[8]
            power_play_goals = stats[9]
            short_handed_goals = stats[10]
            game_winning_goals = stats[11]
            shots = stats[12]
            shot_percentage = stats[13]

            data_skater = {
                'skater_number': int(skater_number),
                'position': str(position),
                'skater_name': str(skater_name),
                'games_played': int(games_played),
                'goals': int(goals),
                'assists': int(assists),
                'points': int(points),
                'plus_minus': int(plus_minus),
                'penalty_minutes': int(penalty_minutes),
                'power_play_goals': int(power_play_goals),
                'short_handed_goals': int(short_handed_goals),
                'game_winning_goals': int(game_winning_goals),
                'shots': int(shots),
                'shot_percentage': float(shot_percentage),}

            add_skater_stats = (
                'INSERT INTO %s '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                ) % (team, skater_number, position, skater_name, games_played, goals, assists, points,plus_minus, penalty_minutes, power_play_goals, short_handed_goals,game_winning_goals, shots, shot_percentage)

            try:
                logging.info("\tINSERTING {}'S PLAYER STATS INTO {} TEAM TABLE".format(skater_name.upper(), team.upper()))
                cursor.execute(add_skater_stats, data_skater)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tMYSQL ERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(skater_name.upper()))

        for row in soup('table')[2].findAll('tr')[1:-1]:
            stats = row.text.strip().encode('ascii').split('\n')

            goalie_number = stats[0]
            goalie_name = stats[1]
            games_played_in = stats[2]
            games_started = stats[3]
            minutes = stats[4]
            goals_against_average = stats[5]
            wins = stats[6]
            losses = stats[7]
            overtime_losses = stats[8]
            shutouts = stats[9]
            shots_against = stats[10]
            goals_against = stats[11]
            save_percentage = stats[12]
            goalie_goals = stats[13]
            goalie_assists = stats[14]
            goalie_penalty_minutes = stats[15]

            data_goalie = {
                'Goalie_Number': int(goalie_number),
                'Goalie_Name': str(goalie_name),
                'Team': str(team.upper()[0]),
                'Games_Played_In': int(games_played_in),
                'Games_Started': int(games_started),
                'Minutes': int(minutes),
                'Goals_Against_Average': float(goals_against_average),
                'Wins': int(wins),
                'Losses': int(losses),
                'Overtime_Losses': int(overtime_losses),
                'Shutouts': int(shutouts),
                'Shots_Against': int(shots_against),
                'Goals_Against': int(goals_against),
                'Save_Percentage': float(save_percentage),
                'Goalie_Goals': int(goalie_goals),
                'Goalie_Assists': int(goalie_assists),
                'Goalie_Penalty_Minutes': int(goalie_penalty_minutes),}

            add_goalie_stats = (
                'INSERT INTO `GOALIES` '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ) % (goalie_number, goalie_name, team, games_played_in, games_started,minutes,goals_against_average, wins, losses, overtime_losses, shutouts, shots_against,goals_against, save_percentage, goalie_goals, goalie_assists,goalie_penalty_minutes)

            try:
                logging.info("\tINSERTING {}'S GOALIE STATS INTO GOALIE TABLE".format(goalie_name.upper()))
                cursor.execute(add_goalie_stats, data_goalie)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(goalie_name.upper()))
    for item in urls_skater4_goalie5:
        page = urllib2.urlopen(item).read()
        soup = bs(page, 'lxml')
        team = item.split('http://')[1].split('.')[0]

        try:
            print 'CREATING {} TABLE'.format(team)
            logging.info('CREATING {} TABLE TABLE'.format(team.upper()))
            cursor.execute(skater_table_sql % team)
        except Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error('\t{} TABLE EXISTS'.format(team.upper()))
            else:
                logging.error('\t' + err.msg.upper())
        else:
            logging.info('\t{} TEAM TABLE CREATED'.format(team.upper()))

        for row in soup('table')[4].findAll('tr')[1:]:
            stats = row.text.strip().encode('ascii').split('\n')

            skater_number = stats[0]
            position = stats[1]
            skater_name = stats[2]
            games_played = stats[3]
            goals = stats[4]
            assists = stats[5]
            points = stats[6]
            plus_minus = stats[7]
            penalty_minutes = stats[8]
            power_play_goals = stats[9]
            short_handed_goals = stats[10]
            game_winning_goals = stats[11]
            shots = stats[12]
            shot_percentage = stats[13]

            data_skater = {
                'skater_number': int(skater_number),
                'position': str(position),
                'skater_name': str(skater_name),
                'games_played': int(games_played),
                'goals': int(goals),
                'assists': int(assists),
                'points': int(points),
                'plus_minus': int(plus_minus),
                'penalty_minutes': int(penalty_minutes),
                'power_play_goals': int(power_play_goals),
                'short_handed_goals': int(short_handed_goals),
                'game_winning_goals': int(game_winning_goals),
                'shots': int(shots),
                'shot_percentage': float(shot_percentage),}

            add_skater_stats = (
                'INSERT INTO %s '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                ) % (team, skater_number, position, skater_name, games_played, goals, assists, points, plus_minus, penalty_minutes, power_play_goals, short_handed_goals, game_winning_goals, shots,shot_percentage)

            try:
                logging.info("\tINSERTING {}'S PLAYER STATS INTO {} TEAM TABLE".format(skater_name.upper(), team.upper()))
                cursor.execute(add_skater_stats, data_skater)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tMYSQL ERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(skater_name.upper()))

        for row in soup('table')[5].findAll('tr')[1:]:
            stats = row.text.strip().encode('ascii').split('\n')

            goalie_number = stats[0]
            goalie_name = stats[1]
            games_played_in = stats[2]
            games_started = stats[3]
            minutes = stats[4]
            goals_against_average = stats[5]
            wins = stats[6]
            losses = stats[7]
            overtime_losses = stats[8]
            shutouts = stats[9]
            shots_against = stats[10]
            goals_against = stats[11]
            save_percentage = stats[12]
            goalie_goals = stats[13]
            goalie_assists = stats[14]
            goalie_penalty_minutes = stats[15]

            data_goalie = {
                'Goalie_Number': int(goalie_number),
                'Goalie_Name': str(goalie_name),
                'Team': str(team.upper()[0]),
                'Games_Played_In': int(games_played_in),
                'Games_Started': int(games_started),
                'Minutes': int(minutes),
                'Goals_Against_Average': float(goals_against_average),
                'Wins': int(wins),
                'Losses': int(losses),
                'Overtime_Losses': int(overtime_losses),
                'Shutouts': int(shutouts),
                'Shots_Against': int(shots_against),
                'Goals_Against': int(goals_against),
                'Save_Percentage': float(save_percentage),
                'Goalie_Goals': int(goalie_goals),
                'Goalie_Assists': int(goalie_assists),
                'Goalie_Penalty_Minutes': int(goalie_penalty_minutes),}

            add_goalie_stats = (
                'INSERT INTO `GOALIES` '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ) % (goalie_number, goalie_name, team, games_played_in, games_started, minutes, goals_against_average, wins, losses, overtime_losses, shutouts, shots_against,goals_against, save_percentage, goalie_goals, goalie_assists, goalie_penalty_minutes)

            try:
                logging.info("\tINSERTING {}'S GOALIE STATS INTO GOALIE TABLE".format(goalie_name.upper()))
                cursor.execute(add_goalie_stats, data_goalie)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(goalie_name.upper()))
    for item in urls_skater2_goalie3:
        page = urllib2.urlopen(item).read()
        soup = bs(page, 'lxml')
        team = item.split('http://')[1].split('.')[0]

        try:
            print 'CREATING {} TABLE'.format(team)
            logging.info('CREATING {} TABLE TABLE'.format(team.upper()))
            cursor.execute(skater_table_sql % team)
        except Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error('\t{} TABLE EXISTS'.format(team.upper()))
            else:
                logging.error('\t' + err.msg.upper())
        else:
            logging.info('\t{} TEAM TABLE CREATED'.format(team.upper()))

        for row in soup('table')[2].findAll('tr')[1:]:
            stats = row.text.strip().encode('ascii').split('\n')

            skater_number = stats[0]
            position = stats[1]
            skater_name = stats[2]
            games_played = stats[3]
            goals = stats[4]
            assists = stats[5]
            points = stats[6]
            plus_minus = stats[7]
            penalty_minutes = stats[8]
            power_play_goals = stats[9]
            short_handed_goals = stats[10]
            game_winning_goals = stats[11]
            shots = stats[12]
            shot_percentage = stats[13]

            data_skater = {
                'skater_number': int(skater_number),
                'position': str(position),
                'skater_name': str(skater_name),
                'games_played': int(games_played),
                'goals': int(goals),
                'assists': int(assists),
                'points': int(points),
                'plus_minus': int(plus_minus),
                'penalty_minutes': int(penalty_minutes),
                'power_play_goals': int(power_play_goals),
                'short_handed_goals': int(short_handed_goals),
                'game_winning_goals': int(game_winning_goals),
                'shots': int(shots),
                'shot_percentage': float(shot_percentage),}

            add_skater_stats = (
                'INSERT INTO %s '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                ) % (team, skater_number, position, skater_name, games_played, goals, assists, points,plus_minus, penalty_minutes, power_play_goals, short_handed_goals,game_winning_goals, shots, shot_percentage)

            try:
                logging.info("\tINSERTING {}'S PLAYER STATS INTO {} TEAM TABLE".format(skater_name.upper(), team.upper()))
                cursor.execute(add_skater_stats, data_skater)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tMYSQL ERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(skater_name.upper()))

        for row in soup('table')[3].findAll('tr')[1:]:
            stats = row.text.strip().encode('ascii').split('\n')

            goalie_number = stats[0]
            goalie_name = stats[1]
            games_played_in = stats[2]
            games_started = stats[3]
            minutes = stats[4]
            goals_against_average = stats[5]
            wins = stats[6]
            losses = stats[7]
            overtime_losses = stats[8]
            shutouts = stats[9]
            shots_against = stats[10]
            goals_against = stats[11]
            save_percentage = stats[12]
            goalie_goals = stats[13]
            goalie_assists = stats[14]
            goalie_penalty_minutes = stats[15]

            data_goalie = {
                'Goalie_Number': int(goalie_number),
                'Goalie_Name': str(goalie_name),
                'Team': str(team.upper()[0]),
                'Games_Played_In': int(games_played_in),
                'Games_Started': int(games_started),
                'Minutes': int(minutes),
                'Goals_Against_Average': float(goals_against_average),
                'Wins': int(wins),
                'Losses': int(losses),
                'Overtime_Losses': int(overtime_losses),
                'Shutouts': int(shutouts),
                'Shots_Against': int(shots_against),
                'Goals_Against': int(goals_against),
                'Save_Percentage': float(save_percentage),
                'Goalie_Goals': int(goalie_goals),
                'Goalie_Assists': int(goalie_assists),
                'Goalie_Penalty_Minutes': int(goalie_penalty_minutes),}

            add_goalie_stats = (
                'INSERT INTO `GOALIES` '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ) % (goalie_number, goalie_name, team, games_played_in, games_started, minutes,goals_against_average, wins, losses, overtime_losses, shutouts, shots_against,goals_against, save_percentage, goalie_goals, goalie_assists,goalie_penalty_minutes)

            try:
                logging.info("\tINSERTING {}'S GOALIE STATS INTO GOALIE TABLE".format(goalie_name.upper()))
                cursor.execute(add_goalie_stats, data_goalie)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(goalie_name.upper()))

    for item in special_url:
        page = urllib2.urlopen(item).read()
        soup = bs(page, 'lxml')
        team = item.split('http://')[1].split('.')[0]

        try:
            print 'CREATING {} TABLE'.format(team)
            logging.info('CREATING {} TABLE TABLE'.format(team.upper()))
            cursor.execute(skater_table_sql % team)
        except Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error('\t{} TABLE EXISTS'.format(team.upper()))
            else:
                logging.error('\t' + err.msg.upper())
        else:
            logging.info('\t{} TEAM TABLE CREATED'.format(team.upper()))

        for row in soup('table')[0].findAll('tr')[1:-1]:
            stats = row.text.strip().encode('ascii').split('\n')

            skater_number = stats[0]
            position = stats[1]
            skater_name = stats[2]
            games_played = stats[3]
            goals = stats[4]
            assists = stats[5]
            points = stats[6]
            plus_minus = stats[7]
            penalty_minutes = stats[8]
            power_play_goals = stats[9]
            short_handed_goals = stats[10]
            game_winning_goals = stats[11]
            shots = stats[12]
            shot_percentage = stats[13]

            data_skater = {
                'skater_number': int(skater_number),
                'position': str(position),
                'skater_name': str(skater_name),
                'games_played': int(games_played),
                'goals': int(goals),
                'assists': int(assists),
                'points': int(points),
                'plus_minus': int(plus_minus),
                'penalty_minutes': int(penalty_minutes),
                'power_play_goals': int(power_play_goals),
                'short_handed_goals': int(short_handed_goals),
                'game_winning_goals': int(game_winning_goals),
                'shots': int(shots),
                'shot_percentage': float(shot_percentage),}

            add_skater_stats = (
                'INSERT INTO %s '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                ) % (team, skater_number, position, skater_name, games_played, goals, assists, points,plus_minus, penalty_minutes, power_play_goals, short_handed_goals,game_winning_goals, shots, shot_percentage)

            try:
                logging.info("\tINSERTING {}'S PLAYER STATS INTO {} TEAM TABLE".format(skater_name.upper(), team.upper()))
                cursor.execute(add_skater_stats, data_skater)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tMYSQL ERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(skater_name.upper()))

        for row in soup('table')[1].findAll('tr')[1:]:
            stats = row.text.strip().encode('ascii').split('\n')

            goalie_number = stats[0]
            goalie_name = stats[1]
            games_played_in = stats[2]
            games_started = stats[3]
            minutes = stats[4]
            goals_against_average = stats[5]
            wins = stats[6]
            losses = stats[7]
            overtime_losses = stats[8]
            shutouts = stats[9]
            shots_against = stats[10]
            goals_against = stats[11]
            save_percentage = stats[12]
            goalie_goals = stats[13]
            goalie_assists = stats[14]
            goalie_penalty_minutes = stats[15]

            data_goalie = {
                'Goalie_Number': int(goalie_number),
                'Goalie_Name': str(goalie_name),
                'Team': str(team.upper()[0]),
                'Games_Played_In': int(games_played_in),
                'Games_Started': int(games_started),
                'Minutes': int(minutes),
                'Goals_Against_Average': float(goals_against_average),
                'Wins': int(wins),
                'Losses': int(losses),
                'Overtime_Losses': int(overtime_losses),
                'Shutouts': int(shutouts),
                'Shots_Against': int(shots_against),
                'Goals_Against': int(goals_against),
                'Save_Percentage': float(save_percentage),
                'Goalie_Goals': int(goalie_goals),
                'Goalie_Assists': int(goalie_assists),
                'Goalie_Penalty_Minutes': int(goalie_penalty_minutes),}

            add_goalie_stats = (
                'INSERT INTO `GOALIES` '
                'VALUES (%s,"%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ) % (goalie_number, goalie_name, team, games_played_in, games_started, minutes,goals_against_average, wins, losses, overtime_losses, shutouts, shots_against,goals_against, save_percentage, goalie_goals, goalie_assists,goalie_penalty_minutes)

            try:
                logging.info("\tINSERTING {}'S GOALIE STATS INTO GOALIE TABLE".format(goalie_name.upper()))
                cursor.execute(add_goalie_stats, data_goalie)
            except mysql.connector.IntegrityError as err:
                logging.error("\t\tERROR: {}".format(err))
            else:
                logging.info("\t\t{} STATS INSERT SUCCESSFUL".format(goalie_name.upper()))
conn.commit()
conn.close()
logging.info('CONNECTION CLOSED----------------------------------------')