import urllib2
from bs4 import BeautifulSoup as bs
import csv
import sys
 
urls0_1 = {}
 
special_url = {'predators':'http://predators.nhl.com/club/stats.htm?gameType=2&season=20152016'}
r = {'rangers':'http://rangers.nhl.com/club/stats.htm?gameType=2&season=20152016&srt=pnum'}#'rangers':'http://rangers.nhl.com/club/stats.htm?gameType=2&season=20152016'} #first one is sorted by number
urls_skater0_goalie1 = {'senators':'http://senators.nhl.com/club/stats.htm', 'blackhawks':'http://blackhawks.nhl.com/club/stats.htm?gameType=2&season=20152016','hurricanes':'http://hurricanes.nhl.com/club/stats.htm', 'ducks':'http://ducks.nhl.com/club/stats.htm?gameType=2&season=20152016', 'mapleleafs':'http://mapleleafs.nhl.com/club/stats.htm', 'avalanche':'http://avalanche.nhl.com/club/stats.htm', 'canadiens':'http://canadiens.nhl.com/club/stats.htm?gameType=3&season=20152016', 'bruins':'http://bruins.nhl.com/club/stats.htm', 'sharks':'http://sharks.nhl.com/club/stats.htm?gameType=2&season=20152016','sabres':'http://sabres.nhl.com/club/stats.htm','coyotes':'http://coyotes.nhl.com/club/stats.htm', 'flames':'http://flames.nhl.com/club/stats.htm','stars':'http://stars.nhl.com/club/stats.htm?gameType=2&season=20152016', 'wild':'http://wild.nhl.com/club/stats.htm?gameType=2&season=20152016','canucks':'http://canucks.nhl.com/club/stats.htm','penguins':'http://penguins.nhl.com/club/stats.htm?gameType=2&season=20152016', 'lightning':'http://lightning.nhl.com/club/stats.htm?gameType=2&season=20152016','capitals':'http://capitals.nhl.com/club/stats.htm?gameType=2&season=20152016','flyers':'http://flyers.nhl.com/club/stats.htm?gameType=2&season=20152016'}

#predators belongs in 0_1
#rangers belongs in 1_2
urls_skater1_goalie2 = {'devils':'http://devils.nhl.com/club/stats.htm?gameType=2&season=20152016',  'panthers':'http://panthers.nhl.com/club/stats.htm?gameType=2&season=20152016','redwings':'http://redwings.nhl.com/club/stats.htm?gameType=2&season=20152016', 'blues':'http://blues.nhl.com/club/stats.htm?gameType=2&season=20152016','jets':'http://jets.nhl.com/club/stats.htm','bluejackets':'http://bluejackets.nhl.com/club/stats.htm','islanders':'http://islanders.nhl.com/club/stats.htm?gameType=2&season=20152016'} #GOOD

# for row in soup('table')[4]/[5].findAll('tr'): #oilers good
# for row in soup('table')[2]/[3].findAll('tr'): # kings good
# for row in soup('table')[1]/[2].findAll('tr'):  #devils good
#for row in soup('table')[1]/[2].findAll('tr'): #rangers good
#for row in soup('table')[1]/[2].findAll('tr'): #panthers good
# for row in soup('table')[1]/[2].findAll('tr'): #red wings good
#for row in soup('table')[1]/[2].findAll('tr'): #blues  
#soup must change based on team
 
urls_skater4_goalie5 = {'oilers':'http://oilers.nhl.com/club/stats.htm?gameType=2&season=20152016'} #GOOD
urls_skater2_goalie3 = {'kings':'http://kings.nhl.com/club/stats.htm?gameType=2&season=20152016'} #GOOD

#note for predators: use for row in soup('table')[0].findAll('tr')[1:-1] and manually add last one
#note for rangers:  use for row in soup('table')[1].findAll('tr')[1:-4] skaters when sorted by number, ...('table')[2].findAll('tr')[1:-1] goalies enter manually later

for key, value in urls_skater0_goalie1.iteritems():
    team = key
    page	= urllib2.urlopen(value).read()
    soup	=bs(page,  'lxml')
    
    for row in soup('table')[0].findAll('tr')[1:]:
        stats = row.text.strip().encode('ascii').split('\n') 

        skater_number=stats[0]
        position=stats[1]
        skater_name=stats[2]
        games_played=stats[3]
        goals=stats[4]
        assists=stats[5]
        points=stats[6]
        plus_minus = stats[7]
        penalty_minutes=stats[8]
        power_play_goals=stats[9]
        short_handed_goals=stats[10]
        game_winning_goals=stats[11]
        shots=stats[12]
        shot_percentage=stats[13]
        
        SKATER = (skater_number, position, skater_name, games_played, goals, assists, points, plus_minus, penalty_minutes, power_play_goals, short_handed_goals, game_winning_goals, shots, shot_percentage)
        
    for row in soup('table')[1].findAll('tr')[1:]:
        stats = row.text.strip().encode('ascii').split('\n') 

        goalie_number = stats[0]
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
         
        GOALIE = (goalie_number, goalie_name, games_played_in, games_started, minutes, goals_against_average, wins, losses, overtime_losses, shutouts, shots_against, goals_against, save_percentage, goalie_goals, goalie_assists, goalie_penalty_minutes, team)
        
        # print GOALIE
        
for key, value in r.iteritems():
    team = key
    page	= urllib2.urlopen(value).read()
    soup	=bs(page,  'lxml')
    for row in soup('table')[1].findAll('tr')[1:-4]:
        stats = row.text.strip().encode('ascii').split('\n') 

        skater_number=stats[0]
        position=stats[1]
        skater_name=stats[2]
        games_played=stats[3]
        goals=stats[4]
        assists=stats[5]
        points=stats[6]
        plus_minus = stats[7]
        penalty_minutes=stats[8]
        power_play_goals=stats[9]
        short_handed_goals=stats[10]
        game_winning_goals=stats[11]
        shots=stats[12]
        shot_percentage=stats[13]
        
        SKATER = (skater_number, position, skater_name, games_played, goals, assists, points, plus_minus, penalty_minutes, power_play_goals, short_handed_goals, game_winning_goals, shots, shot_percentage)
        
    for row in soup('table')[2].findAll('tr')[1:-1]:
        stats = row.text.strip().encode('ascii').split('\n') 

        goalie_number = stats[0]
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
         
        GOALIE = (goalie_number, goalie_name, games_played_in, games_started, minutes, goals_against_average, wins, losses, overtime_losses, shutouts, shots_against, goals_against, save_percentage, goalie_goals, goalie_assists, goalie_penalty_minutes, team)
        # print GOALIE
        
for key, value in urls_skater1_goalie2.iteritems():
    team = key
    page	= urllib2.urlopen(value).read()
    soup	=bs(page,  'lxml')
    for row in soup('table')[1].findAll('tr')[1:-4]:
        stats = row.text.strip().encode('ascii').split('\n') 

        skater_number=stats[0]
        position=stats[1]
        skater_name=stats[2]
        games_played=stats[3]
        goals=stats[4]
        assists=stats[5]
        points=stats[6]
        plus_minus = stats[7]
        penalty_minutes=stats[8]
        power_play_goals=stats[9]
        short_handed_goals=stats[10]
        game_winning_goals=stats[11]
        shots=stats[12]
        shot_percentage=stats[13]
        
        SKATER = (skater_number, position, skater_name, games_played, goals, assists, points, plus_minus, penalty_minutes, power_play_goals, short_handed_goals, game_winning_goals, shots, shot_percentage)

    for row in soup('table')[2].findAll('tr')[1:-1]:
        stats = row.text.strip().encode('ascii').split('\n') 

        goalie_number = stats[0]
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
         
        GOALIE = (goalie_number, goalie_name, games_played_in, games_started, minutes, goals_against_average, wins, losses, overtime_losses, shutouts, shots_against, goals_against, save_percentage, goalie_goals, goalie_assists, goalie_penalty_minutes, team)
print all(GOALIE)
