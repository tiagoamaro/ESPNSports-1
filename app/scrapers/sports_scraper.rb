require "mysql"
require "nokogiri"
require "mechanize"

## TODO
## when stats are hyphen
## seperated they need to follow
## the following logic
## MADE-ATTEMPTED
##
## currentGame variable
## always be the variable
## we are processing currently


## these dfields will be seperated
## in thedatabase so, you need to add the field
## schema logic

## league ids can be found using
## the espn website
##
## whenever adding a newleague add the corresponding
##league id here

## TODO Still needs a way 
## to get the  

LEAGUE_IDS =  {
    "NFL" => "",
    "NBA" => "",
     "NHL" => ""
}

##
## i.e.
## 3PT_Made
## 3PT_Attempted
##


## add is_number to 
## string methods
def is_number(number)
  true if Float(number) rescue false
end

###
## Update, insert strings
## around the MySQL object
##
class DBSyntax 

  def get_schema(db, table)

    sql = "EXPLAIN " + table
    schema = {}
    rows = db.query(sql)
    rows_ = Array.new

    ## get the whole
    ## schema as an arrayq

    while row = rows.fetch_row
      rows_.push(row)
    end

    return rows_
  end

  def foreign_key_checks_off(db)
     db.query("SET FOREIGN_KEY_CHECKS=0;")
  end
  def escape_val(val)
     return val.gsub(/'/, "\\'")
  end
  ## insert something
  ## into the database
  ## @param table a table
  ## belonging to the database
  ##
  def insert_str(db, table, opts)
    db.query("SET FOREIGN_KEY_CHECKS=0;")
    str = String.new("")
    str += "INSERT INTO `" +  table + "` ("
    schema = self.get_schema(db, table)
   
    opts.each { |k,v|  
      if not v then
        v = ""
      end
      if k then 
        str += "`" + String.new(k.to_s) + "`,"
      end
    }
    str = str.gsub(/,$/, "")

    str += ") VALUES ("

		puts opts
    opts.each { |k, v|  
      if not v then
        v = ""
      end
      if k and v then
        str += "'" + String.new(self.escape_val(v.to_s)) + "',"
      end
      if not v then
        str += "'',"
      end
      
    }
    str = str.gsub(/,$/, "")

    str += ")"
		puts str
  
    return str 
  end

  ## update str 
  ## 
  ## @param table a table belonging
  ## to the database
  ## @opts list of parameters belonging to the tables schema
  def update_str(db, table, key, value, opts)
    self.foreign_key_checks_off(db)
    schema = self.get_schema(db, table)
    str = String.new("")

    str += "UPDATE `" + table + "` SET "
    opts.each { |k, v|
      if not v then
        v = ""
      end
      if k and v then    
        str += "`" + String.new(k.to_s) + "`" + " = '" + String.new(v.to_s) + "', "
      end
    } 
	 	str = str.gsub(/,\s$/,"")

    str += " WHERE " + "`" + String.new(key.to_s) + "` = '" + String.new(self.escape_val(value.to_s)) + "'"

    puts str
    return str
  end
end

###
## Main scraper
## should retrieve information
## given the 
## league 
class SportsScraper 
    def initialize(league)
      @entrypoints = {}

      @datestr = self.make_time()

      @inheritors = {
        "NBA" => ['WNBA', 'NCAAB', 'NCB', 'NCAAWB', 'NCW'],
        "NFL" => ['NCAAF', 'NCF']
      }
      @entrypoints['NBA'] = {
          ## this should
          ## be the entry point
         #"url" =>  "http://scores.espn.go.com/nba/scoreboard?date=" + @datestr,
         ## testing
         "url" => "http://espn.go.com/nba/scoreboard?date=20150308",

         "league_table" => "Players_NBA",
         "players_table" => "Players_NBA",
         "FriendlyName" => "Basketball",
				 "LeagueName" => "NBA",
         "LeagueID" => 1,
         ## rules
         ## for finding 
         ## schema information
         ## generate this
        ## once qwe've connected to
        ## the database
         "schema" => {},
         "espnSchema" => [
          "Placeholder",
          "Player Profile",
          "Min",
          "FGM-A",
          "3PM-A",
          "FTM-A",
          "OREB",
          "DREB",
          "REB",
          "AST",
          "STL",
          "BLK",
          "TO",
          "PF",
          "Plus Minus",
          "PTS"
          ],
         "splitters" => {
            "FGM-A" => [
                 "FGMade",
                 "FGTaken"
             ],
             "3PM-A" => [
                 "ThreePtMade",
                 "ThreePtTaken"
             ],
             "FTM-A" => [
                 "FTMade",
                 "FTTaken"
             ]
          },

         "trans" => {
            "Placeholder" => "",
            "Min" => "GameStatus", ## no implementation
            "FGM-A" => "", ## done by splitters
            "3PM-A" => "", ## done by splitters
            "FTM-A" => "", ## done by splitters
            "OREB" => "OffRebounds",
            "DREB" => "DefRebounds",
            "REB" => "Rebounds",
            "AST" => "Assists",
            "BLK" => "Blocks",
            "TO" => "Turnovers",
            "PF" => "PersonalFouls",
            "Plus Minus" => ""
         },

         "percents" => {
             "FTPercent" => {
               "lower" => "FTMade",
               "upper" => "FTTaken"
             },   
            "FGPercent" => {
               "lower" => "FGMade",
               "upper" => "FGTaken"
            },
            "ThreePtPercent" => {
               "lower" => "ThreePtMade",
               "upper" => "ThreePtTaken"
            }
         },

         "scorePeriods" => [
            "Quarter_1",
            "Quarter_2",
            "Quarter_3",
            "Quarter_4",
            "Overtime_1",
            "Overtime_2",
            "Overtime_3"
         ] 
      }

      @entrypoints['NASCAR'] = {
        "url" => "http://espn.go.com/racing/schedule/_/series/nationwide",
        "LeagueId" => 2,
        "espnSchema" => [
          "POS",
          "DRIVER",
          "CAR",
          "MANUFACTURER",
          "LAPS",
          "MONEY",
          "START",
          "LED",
          "PTS",
          "PENALTY"
        ],
        "LeagueName" => "NASCAR",
        "FriendlyName" => "Racing",
        "trans" => {
          "POS" => "Position",
          "LAPS" => "LapsComplete",
          "LED" => "LapsLed",
          "MONEY" => "Winnings",
          "START" => "StartPlace",
          "PENALTY" => "Penalty"
        },
        "schema" => {},
        "percents" => {}
      }
      @entrypoints['NFL'] = {
        "LeagueID" => 3,
        "url" =>  "http://scores.espn.go.com/nfl/scoreboard?date=" + @datestr,
        "league_table" =>  "Players_NFL",
        "players_table" => "Players_NFL",
				"LeagueName" => "NFL",
        "FriendlyName" => "Football",
        "playerFinding" => 0,
        "espnSchema" => {
       
            "Passing" => [ 
               "C/ATT", 
               "YDS",
               "AVG",
               "TD",
               "INT",
               "SACKS",
               "QBR",
               "RTG"
             ],
            "Rushing" => [
              "CAR",
              "YDS",
              "AVG", 
              "TD",
              "LD"
            ],

            "Receiving" => [
              "REC",
              "YDS",
              "AVG",
              "TD",
              "LG",
              "TGTS"
            ],

            "Defensive" => [
              "TOT",
              "SOLO",
              "SACKS",
              "TFL",
              "PD",
              "QB HITS",
              "TD"
            ],
    
            "Interceptions" => [
              "INT",
              "YDS",
              "AVG",
              "LG",
              "TD"
            ],

            "Returns" => [
              "NO",
             "YDS",
              "AVG",
              "LG",
              "TD"
            ],


            "Kick Returns" => [
              "NO",
              "YDS",
              "AVG",
              "LG",
              "TD"
            ],
            "Punt Returns" => [

              "NO",
              "YDS",
              "AVG",
              "LG",
              "TD"
            ],

            "Kicking" => [
              "FG",
              "PCT",
              "LONG",
              "XP",
              "PTS"
            ],

            "Punting" => [
              "NO",
              "YDS",
              "AVG",
              "TB",
              "-20",
              "LG"
            ]

        },

        "trans" => {
            "General" => {
                "1st Downs" => "FirstDowns",
                "Passing 1st Downs" => "1st_downs",
                "Rushing 1st downs" => "1st_downs",
                "1st downs from penalties" => "",
                "3rd down from efficiency" => "",
                "4th down efficiency" => "",
                "Total Yards" => "",
                "Turnovers" => "",
                "Fumbles lost" => "FumblesLost",
                "Interceptions Thrown" => ""
  
             },
            "Passing" => {
               "TD" =>  "PassingTDs",
               "INT" => "PassingInterceptions",
               "RTG" => "PassingRTG",
               "SACKS" => "PassingSacks"
            },
            "Rushing" => {
              "TD" => "RushingTDs",
              "LG" => "RushingLong",
              "YD" => "RushingYards",
            },
            "Receiving" => {
              "REC" => "ReceivingCatches",
              "YD" => "ReceivingYards",
              "LG" => "ReceivingLong",
              "TD" => "ReceivingTDs",
              "TGTS" => "ReceivingTargets",
            },
            "Defensive" => {
              "SACKS" => "DefenseSacks",
              "SOLO" => "DefenseSolos",
              "TDS" => "DefenseIntTDS",
              "PD" => "DefensePassesDefended",
              "QB HTS" => "",
              #"TFL" => "DefenseTackles",
              "TOT" => "DefenseTackles"
            },
            "Interceptions" => {
               "INT" => "DefenseInterceptions",
               #"YDS" => "InterceptionYards",
               #"TD" => "InterceptionTDs"
            },
            "Punt Returns" => {
                "TD" => "PuntReturnTDs",
                "LG" => "PuntReturnLong",
                "YDS" => "PuntReturnYards",
                "AVG" => "",
                "TD" => ""
            },
            "Kick Returns" => {
              "NO" => "KickReturns",
              "FG" => "",
              #"PCT" => "",
              #"LONG" => "",
              "XP" => "",
              "LG" => "KickReturnLong",
              "TDS" => "KickReturnTDs",
              "YDS" => "KickReturnYards",
              "PTS" => ""
            },
            "Kicking" => {
                "FG" => "",
                "PCT" => "",
                "LONG" => "",
                "XP" => "",
                "PTS" => ""
            },
            "Punting" => {
              "NO" => "Punts",
              "YDS" => "PuntYards",
              "AVG" => "",
              "TB" => "",
              "TB/s" => "",
              "-20" => "",
              "LG" => ""
            }
            
        },
        "splitters" => {
            "Kicking" => {
              "FG" => {
                  "data" => [
                     "KickingFGAttempts", 
                     "KickingFGMade"
                  ],
                  "delimiter" => "/"
              }
            },  

            "Passing" => {
              "C/ATT" => { "data" => [
                "PassingCompletions",
                "PassingAttempts"
                ],
                "delimiter" => "/"
              } 
            }
        },

        "percents" => {
            "KickingFGPct" => {
              "lower" => "KickingFGMade",
              "upper" => "KickingFGAttempted"
            },
            "PassingCompletionPct" => {
              "lower" => "PassingCompletions",
              "upper" => "PassingAttempts"
            }
        },

        "scorePeriods" => [
            "Quarter_1", 
            "Quarter_2",
            "Quarter_3",
            "Quarter_4",
            "Overtime_1",
            "Overtime_2"
        ],

        "schema" => {}
      }


      ## same as NBA
      ##
      @entrypoints['NCB'] = {
        "url"  => "http://scores.espn.go.com/ncb/scoreboard?date=20150329",
        "LeagueName" => "NCB",
        "FriendlyName" => "Basketball",
        "espnSchema" => [
        ],
        "trans" => {},
        "schema" => {} 

      
      }
      @entrypoints['NCW'] = {
        "url" => "http://scores.espn.go.com/ncw/scoreboard?date=20150213",
        "FriendlyName" => "Basketball",
        ## same as NBA
        "espnSchema" => [

        ],
        "schema" => {}
      }

      @entrypoints['WNBA'] = {
        "LeagueID" => 8,
        "url" => "http://scores.espn.go.com/wnba/scoreboard?date=20140502",
        "FriendlyName" => "Basketball", 

         ## same as NBA
        "espnSchema" => [

         ],

        "percents" => {},
        "splitters" => {},
        "schema" => {}
      }
      @entrypoints['NCF'] = {
        "LeagueID" => 7,
        "url" => "http://scores.espn.go.com/ncf/scoreboard?date=20150112",
        "FriendlyName" => "Football",
        "LeagueName" => "NCF",

        ## should be same
        ## as NFL
         "espnSchema" => [

          ],
        "schema" => {}      
      }
      @entrypoints['MLS'] = {
        "LeagueID" => 9,
        "url" => "http://www.espnfc.us/scores?date=20150325",
        "BaseURL" => "http://www.espnfc.us/",
        "FriendlyName" => "Soccer",
        "PlayerBaseURL" => "http://www.espnfc.us/",
        "scorePeriods" => [

        ],
        "espnSchema" => [
          "POS",
          "NO",
          "Name",
          "SH",
          "SG",
          "G",
          "A",
          "OF",
          "FD",
          "FC",
          "SV",
          "YC",
          "RC"
        ],
        "trans" => {
          "G" => "Goals",
          "A" => "Assists",
          "FC" => "FoulsCommited",
          "FD" => "FoulsSuffered",
          "YC" => "YellowCards",
          "SH" => "Shots",
          "SV" => "Saves",
          "RC" => "RedCards",
        },
        "percents" => {

        },
        "schema" => {}
      }


      @entrypoints['PGA'] = {
        "url" => "http://espn.go.com/golf/leaderboard",
        "LeagueID" => 6,
        "FriendlyName" => "Golf",
        "LeagueName" => "PGA",
        "scorePeriods" => [
          
        ],
        "espnSchema" => [
          "POS",
          "CNTRY",
          "PLAYER",
          "TO PAR",
          "R1",
          "R2",
          "R3",
          "R4",
          "TOT",
          "EARNINGS",
          "FEDEX PTS"
        ],
        "schema" =>  {},
        "percents" => {},
        "trans" => {
          "POS" => "Position",
          "R1" => "Round1",
          "R2" => "Round2",
          "R3" => "Round3",   
          "R4" => "Round4",
          "TOT" => "Strokes",
          "THRU" => "MissedCuts",
          "TO PAR" => "ToPar"

        }
      }

      @entrypoints['MLB'] = {
        "LeagueID" => 4,
        "url" => "http://espn.go.com/mlb/scoreboard?date=20130601",
        #"url" =>  "http://scores.espn.go.com/nhl/scoreboard?date=20150325",
        "FriendlyName" => "Baseball",
        "LeagueName" => "MLB",
        "scorePeriods" => [
            "Inning_1", 
            "Inning_2",
            "Inning_3",
            "Inning_4",
            "Inning_5",
            "Inning_6",
            "Inning_7",
            "Inning_8",
            "Inning_9",
            "Inning_10",
            "Inning_11",
            "Inning_12",
            "Inning_13",
            "Inning_14"
        ],
        "espnSchema" => {
            "Batters" => [
              "AB",
              "R",
              "H",
              "RBI",
              "BB",
              "SO",
              "#P",
              "AVG",
              "OBP",
              "SLG"
            ],
            "Pitchers" => [
              "IP",
              "H",
              "R",
              "ER",
              "BB",
              "SO",
              "HR",
              "PC-ST",
              "ERA"
            ]
         },
         "trans" => {
           "Pitchers" => {
                "HR" => "PitchingHomeRuns", 
                "IP" => "PitchingInnings",
                "ER" => "PitchingEarnedRuns",
                "R" => "PitchingRuns",
                "RBI" => "RBI",
                "SO" => "PitchingStrikeouts",
            },
            "Batters" => {
                "AB" => "AtBats",
                "R" => "Runs",
                "HR" => "Homeruns"
            }
         },
         "splitters" => {
            "Pitchers" => {

            },
            "Batters" => {

            }
         },
         "percents" => {

         },
        
         "schema" => {}
      }
      @entrypoints['NHL'] = {
        "LeagueID" => 5,
        #"url" =>  "http://scores.espn.go.com/nhl/scoreboard?date=" + @datestr,
        "url" => "http://scores.espn.go.com/nhl/scoreboard?date=20150325",
        "league_table" => "Players_NHL",
        "FriendlyName" => "Hockey",
				"LeagueName" => "NHL",
        "players_table" => "Leagues_NHL",
        "schema" => {},
        "espnSchema" => { 
            "Players" => [
              "G",   
              "A",
              "+/-",
              "SOG",
              "MS",
              "BS",
              "PN",
              "PIM",
              "HT",
              "TK",
              "GV",
              "SHG",
              "TOT",
              "PF",
              "PP",
              "EV",
              "FW",
              "FL",
              "%"
            ],
            "Goalies" => [
              "SA",
              "GH",
              "SAVES",
              "SV%",
              "TOI",
              "PM"
            ]
          },
          "scorePeriods" => [
              "Period_1",
              "Period_2",
              "Period_3",
              "Overtime_1",
              "Overtime_2"
    
          ],
          "splitters" => {

           },
          "trans" => {
              "Players" => {
                "SOG" => "ShotsOnGoal",
                "FW" => "FaceoffsWon",
                "FL" => "FaceoffsLost",
                "PIM" => "PenaltyMinutes",
                "G" => "Points",
                "BS" => "Blocks",
                "A" => "Assists",
                "HT" => "Hits",
                "GV" => "Giveaways",
                "TK" => "Takeaways",
                "MS" => ""
              },
              "Goalies" => {
                "SAVES" => "Saves",
                "SA" => "ShotsAgainst",
                "SOG" => "ShotsOnGoal",
                "PIM" => "PenaltyMinutes"
                  ## todo 
              }
          },
          "percents" => {
              "FaceoffPercent" => {
                 "type" => "sum",
                 "data" => ["FaceoffsWon", "FaceoffsLost"],
                 "matcher" => "FaceoffsWon"  
              }
          }
      }
      @endpoints = {
        "stats" =>  1,
        "scoreboard" => 1,
        "tickets" => 1
      }
     @league = league

      trans = {
           "NCAAF" => "NCF",
           "NCAAB" => "NCB",
           "NCAAWB" =>  "NCW"
    
      } 
        ## translate any league
      ## names
      if trans.keys.include? league then
          @league = trans[league]
      end

      ## usually we need
      ## to keep one match
      ## url, and it needs the
      ## game id

      if league == "PGA" then
        @match_url = "http://espn.go.com/golf/leaderboard?tournamentId="
      elsif league == "NASCAR" 
        @match_url = "http://espn.go.com/racing/raceresults?raceId="
      elsif league == "MLS" 
        @match_url = "http://www.espnfc.us/gamecast/statistics/id/"
      else
        @match_url = "http://espn.go.com/{league}/{endpoint}?gameId="
      end

            

      #if @entrypoints.has_key(@league)?
      @entrypoint = @entrypoints[@league]


      @leagueId = @entrypoint['LeagueID']

      puts "League Id is: "
      puts @leagueId
      @leagueFriendlyName  = @entrypoint['FriendlyName']
      @scorePeriods = @entrypoint['scorePeriods']

      #@entrypoints.each { |k, entrypoint|
      @inheritors.each {  |parent, children|
          keys = ['espnSchema', 'schema', 'percents', 'splitters', 'trans', 'scorePeriods'] 
          children.each { |c|
              if c ==  @league then 
  
                keys.each {  |k|
                   @entrypoint[k] = @entrypoints[parent][k]
                }

              end
          }

      } 

      @scorePeriods = @entrypoint['scorePeriods']

      #}
      ## main db connection
      ## 
      rails_db_config = Rails.application.config.database_configuration[Rails.env]
      @host = rails_db_config['host']
      @username = rails_db_config['username']
      @pass = rails_db_config['password']
      @port = rails_db_config['port']
      @table = rails_db_config['database']
      @db = Mysql.connect(@host, @username, @pass, @table)

      @espnSchemas = @entrypoint['espnSchema']
      @percentages = @entrypoint['percents']
      @splitters = @entrypoint['splitters']
      @trans = @entrypoint['trans']
      @playerFinding = @entrypoint['playerFinding']

      @dbsyntax = DBSyntax.new()
      @schema = @dbsyntax.get_schema(@db, "TeamStats_Hockey")
      @client = Mechanize.new()

      @entrypoints[@league]['schema']['Player'] = @dbsyntax.get_schema(@db, self.get_game_player_table()) 
      @playerSchema = @entrypoints[@league]['schema']['Player']

      if not self.is_singular_league() then 
        @entrypoints[@league]['schema']['Team'] = @dbsyntax.get_schema(@db, self.get_game_team_table())
        @teamSchema = @entrypoints[@league]['schema']['Team']
      else 
        @teamSchema = []
      end
    
      @to_traverse = Array.new()
      @to_traverse_times = {}

    end


    def get_team_info(teamLink)

        team = @client.get(teamLink)
        ## todo
    end

    def get_game_if_exists(gameId)
       query = @db.query("SELECT * FROM `Games` WHERE GameID = '" + gameId + "' LIMIT 1;")
       game = {}

       if query.num_rows > 0 then
         schema = @dbsyntax.get_schema(@db, "Games")
         vals = 0

         while row = query.fetch_row
           cnt = 0
           row.each do |row_value|
             if cnt == 0
               game[schema[vals]] =  row_value
                 vals += 1
             end
           end
           cnt = 0
         end
        return game
       end


       return false
    end

    ## get the team id
    ## from a given team
    ## this is available in the
    ## document via teamId= 
    def get_team_id(team_url)
        res = @client.get(team_url)
        parser = res.parser
        puts team_url
        matches = parser.inner_html.match(/\?teamId=(\d+)/) 

        return matches[1]
    end


    ## basketball 
    ## stats
    ##
    ##
    def process_basketball_stats(mod_data)
			 home_players_1 = mod_data.children[1].children
			 home_players_2 = mod_data.children[3].children
			 away_players_1 = mod_data.children[5].children
			 away_players_2  = mod_data.children[7].children
		 	 away_players_3 = mod_data.children[9].children
			 home_players = [] 
			 away_players = [] 
       parser = @parser
       hash = @espnSchemas
       splitters = @splitters
       percents = @percentages
       trans = @trans
			
       #odd_players = parser.xpath("//tr[contains(@class, 'odd player')]")       
       #even_players = parser.xpath("//tr[contains(@class, 'even player')]")
			 home_players_1.each { |player|
					 home_players.push({
							"element" => player,
							"teamId" => @home_team_id
						})
			 }
			 home_players_2.each { |player|
					home_players.push({
						"element" => player,
						"teamId" => @home_team_id
					})
			 }
			 away_players_1.each { |player| 
					away_players.push({ 
						"element" => player,
						"teamId" => @away_team_id
					})
			 }
			 away_players_2.each { |player|
					away_players.push({
						"element" => player,
						"teamId" => @away_team_id
					})
			 }
			away_players_3.each { |player|
				away_players.push({
					"element" =>player,
					"teamId" => @away_team_id
				})
			}

       final_players = home_players + away_players
       players = {}
       final_players.each { |player_|
					player = player_['element']
					teamId = player_['teamId']
          ## needs a way
          ## of finding
          ## which team the
          ## player is one
    
        
          ## first record is always
          ## there name
          cnt = 0
          name = ''
          stats =  player.children
					## qwe need to look at the players
					## name and also what his
					## stats are. names are found in first
					## column, get the id which is in the url 

          ## check whether we need 
          ## to split the result
          ## which is usually
          ## defined by 
          ##
          ## {RES1}-{RES2}

          stats.each { |stat|
            if cnt > 1 and not cnt > hash.length and stat then
              if players[name] then

                if splitters[hash[cnt]] then 
                 
                   check = splitters[hash[cnt]]
                   splits = stat.inner_html.match(/(\d+)-(\d+)/)
                   first = check[0] 
                   second = check[1]
                  if  splits then
                   players[name][first] = splits[1]
                   players[name][second] = splits[2]
                  end
                   ## todo  
  
                else
                  players[name][trans[hash[cnt]]] = stat.inner_html 
                end
              end
            end
            
            if cnt == 1 then
  
              ## some leagues
              ## don't have
              ## links to profiles
              ## in players so
              ## we will
              ## only get the name

              child = stat.children[0]

              if child.class.to_s == 'Nokogiri::XML::Element' then
                link =  child.attr("href")

                if link then
                  matches = link.match(/player\/_\/id\/(\d+)/)
                  name = stat.children[0].inner_html
                  players[name] = {  
                    "teamId" => teamId,
                    "id"=> matches[1],
                    "url" => link,
                    "name" => name
                  }
                end
              else
                  name = child.to_s
                  players[name] = {
                    "teamId" => teamId,
                    "id" => "", 
                    "url" => "",
                    "name" => ""
                  }
              end
            end
            cnt += 1
          }
        
        ##  now form the percentages if any
        percents.each { |k, percent|

            if players[name] then
              if players[name][percent['lower']] and players[name][percent['upper']] then
                lower = players[name][percent['lower']].to_f
                upper = players[name][percent['upper']].to_f
       
                if lower > 0 
                  percentage = (lower / upper) * 100 
                else
                  percentage = 0
                end

                players[name][k] = String.new(percentage.to_s) + "%"
              end
            end
        }
       }

      ## now 
      ## we get the team stats, the hash should
      ## look the same as player stats
      ##
      ## first one is home
      ## second one is away
      home_acc = @home_acc
      away_acc = @away_acc
      team_stats = {
      }

      teams = parser.xpath("//*[@class='even']")
      home_stats = teams.first.children
      away_stats = teams.last.children
      cnt = 0
       team_stats[@home_acc] = {}
        team_stats[@away_acc] = {}
      home_stats.each { |stat|
          if cnt > 1  and cnt < hash.length then
       
          ## we need the first 
          ## child which is the strong
          ## element
            if stat.children.length > 0 then
             team_stats[@home_acc][hash[cnt]] =  stat.children[0].inner_html
            end
          end
          ##
          ## we dont need
          ##information
          if cnt == 1 then
          end
          cnt += 1
      } 

      cnt = 0
      away_stats.each { |stat|
          if cnt > 1 and cnt < hash.length then
              #puts stat.inner_html
            if stat.children.length > 0 then
              team_stats[@away_acc][hash[cnt]] = stat.children[0].inner_html
            end
          end
          cnt += 1
      }
 
      return {
        "players"=> players,
        "teams"=> team_stats
      }
    end


    ## add a new player to 
    ## the players set
    ## this should need
    ## the players info
    ## and his stats
    ##
    def generate_player(players, player, teamId)
      name = player['name']
      if name then 
        if not players[name] then
          players[name] = {}
        end

        player.each  { |k,v|
          players[name][k] = v
        }
        players[name]['teamId'] = teamId
      end 

      return players
    end


    ## process soccer stats
    ## output should look like
    ##
    ##  players
    ##
    ##
    ## GameID,LeagueID,TeamID,PlayerID,Goals,Saves,GoalsConceded,Assists,Shots,Tackles,Clearances,Corners,FoulsConceded,FoulsSuffered,YellowCards,RedCards,Penalties,Minutes,CreatedDate,ModifiedDate
    ##
    ## teams
    ##
    ##
    ##
    ## GameID,LeagueID,TeamID,FinalScore,Possession,TackleSuccess,PassAccuracy,TotalShots,ShotsOnGoal,Corners,Saves,Offsides,Fouls,YellowCards,RedCards,CreatedDate,ModifiedDate
    ##
    ##
    ## 
    ##
    ## modData is found in two containers
    ## the first being the
    ##
    def process_soccer_stats(modData)
      d =  modData.xpath("table[@class='stat-table']")
      ## soccer structures need
      ## needing for there
      ## data so always skip
      ## one element whenever
      ## looking to find 
      ##
      home_players =  d[2].children[3]
      away_players = d[3].children[3]
 
      teams = {} 
      team_stats = {}     
      home_stats = {}
      away_stats = {}
      final_players = {}
      final_teams = {}
      players = process_struct_of_data_soccer(@espnSchemas, home_players, @trans, "player")
      players.each {  |k, player|

          final_players = self.generate_player(final_players, player, @home_team_id)
      }
      players = process_struct_of_data_soccer(@espnSchemas, away_players, @trans, "players")
      players.each { |k, player|
           final_players = self.generate_player(final_players, player, @away_team_id)
      }
          
      ## todo find alternativew if possible 
      ## having the team glossary would be best
      ## things we are not looking for 
      look_aside = ['id', 'name', 'url', 'teamId']
      final_players.each { |player, struct|
           team_id = player['teamId']  
           struct.each { |k,v|

              if look_aside.include? k then
                next
              end
              if not home_stats[k] then 
                home_stats[k] = 0
              end 
              if not away_stats[k] then
                away_stats[k] = 0
              end

              val = v.to_f 
              if team_id == @home_team_id then
                  home_stats[k] += val 
              else
                  away_stats[k] += val
              end
           } 
      }

      teams[@home_acc] = home_stats
      teams[@away_acc] = away_stats
      return {
        "players" => players,
        "teams" => teams 
      }
    end


    ## different for soccer
    ## stats where we need to consider
    ## things like padding
    ## for each element 
    ## team based statistics
    ## would be processed
    ## seperatrly
    ## 
    ## return playerSet
    def process_struct_of_data_soccer(schema, data, trans, type)
       players = {}
       url_base = 'http://www.espnfc.us/'
       data.children.each { |player|
          if player.class.to_s == "Nokogiri::XML::Element" then
            stats = player.xpath("td")
            ## player info would be
            ## found in area 3
            ##
            ##
         
            if not stats.children.length>1 then 
              next
            end
            player_name = stats[2].children[1].inner_html.to_s
            player_link = stats[2].children[1].attr("href")
            player_id = player_link.match(/id\/_\/(\d+)/)

            
            if player_id then
              player_id = player_id[1]
            end

          
            players[player_name]= {}  
            players[player_name]['id'] = player_id
            players[player_name]['url'] = url_base + player_link
            players[player_name]['name'] =  player_name
            cnt = 0
            stats.each {  |stat|

                ## we need to skip
                ## all the text elements
                ## this is 
                ##
                if not stat.class.to_s == "Nokogiri::XML::Element" then
                  next
                end
                if cnt ==  2 then
                  ## skip the name we have it
                  cnt += 1 
                  next
                end

                stat = stat.inner_html
                ## MLS adds extra padding
                ## we need to take this out
                ##
                stat = stat.gsub(/\n|\r|\s+/, "")

                if trans[schema[cnt]] then
                  players[player_name][trans[schema[cnt]]] = stat 
                end

                cnt += 1

            }
          end
       }
        puts players

      return players
    end
    ## process racing stats 
    ## output should look like
    ##
    ##
    ## GameID,LeagueID,PlayerID,StartPlace,FinishPlace,LapsComplete,LapsLed,Winnings,CreatedDate,ModifiedDate
    ##
    ##
    ##
    ## we can find the player stats
    ## by looking at the
    ## odd players and even players
    ## we will also merge these
    ##
    def process_racing_stats(modData)

        odd_players = modData.xpath("//tr[contains(@class, 'oddrow player')]")
        even_players = modData.xpath("//tr[contains(@class, 'everow player')]")
        players_final = {}
        players = odd_players + even_players
        schema = @espnSchemas
        ## player stats can be found
        ## in the driver column
        ## when 
        ##
        base_url = 'http://espn.go.com'
        players.each { |player|
          stats = player.xpath("td")
          name = stats[1].children[0].inner_html
          link = stats[1].children[0].attr("href")
          id = link.match(/_\/id\/(\d+)/)
      
          players_final[name] = {}
          players_final[name]['url'] =  base_url + link
          players_final[name]['id'] = id[1]
          cnt = 0
          puts stats
          stats.each { |stat|
            if cnt == 1 then 
              cnt += 1
              next
            end

            players_final[name][@trans[schema[cnt]]] = stat.inner_html
            cnt += 1
          }
        }

      ## important for the players
      ## whena  gamer is over the posittion
      ## becomes the 'FinalPosition'
      ##
      ##

      return {
        "players" => players_final,
        "teams" => {}
      }
    end

    ## process golf 
    ## stats out should
    ## look like
    ##
    ## GameID,LeagueID,PlayerID,Position,Round_1,Round_2,Round_3,Round_4,ToPar,Strokes,MissedCut,CreatedDate,ModifiedDate
    ##
    ##
    ## player stats should be found in
    ## the  xpath element @id contain player-
    #
    ##
    ##
    ##
    def process_golf_stats(modData)
        elem = ""
        players = modData.xpath("//tr[contains(@id, 'player-')]")

        players_final = {}
        ## custom process
        ## we need to look
        ## at every attribute
        ## 
        ## third one will be the player's
        ## name
        ##
        ##
        ## base url for players is 
        ## 
        link_base = "http://espn.go.com/golf/player/_/id/"
        players.each { |player|
          stats = player.xpath("td")

          ## for golf we may get
          ## the needed player info
          ## in different indicies as 
          ## a result
          cnts = [0,1,2,3] 
          name=''
          id=''
          cnts.each { |cnt|
            if stats[cnt] then 
              if stats[cnt].children then
                if stats[cnt].children[0].node_name == "a" then
                    id = stats[cnt].children[0].attr("name") 
                    name = stats[cnt].children[0].inner_html
                 end
              end
            end
          } 

  
            
          players_final[name] = {}
          ## to get
          ## the url
          ## we also need to 
          ## downcase
          ## the name andhyphenate the spaces
          sname = name.downcase.gsub(/\s/, "-")
          link = link_base + id + "/"  + sname

          ## no teams
          ## for golf
          players_final[name]['teamId'] = nil
          players_final[name]['url'] = link
          players_final[name]['id'] = id

          cnt = 0
          stats.each { |stat|
            cur = @espnSchemas[cnt]          

            ## we may need to look
            ## in the inner children of the
            ## element so 

            if stat.children[0].class.to_s == "Nokogiri::XML::Element"
              stat = stat.children[0].inner_html.to_s
            else
              stat = stat.inner_html.to_s
            end
            if not cur == "PLAYER" then
              players_final[name][cur] = stat
            end
            cnt += 1
          }
        }     

      return {
        "players" => players_final,
        "teams" => {}
      }
    end
    ## process baseball
    ## stats output
    ## should look like
    ##
    ##
    ##   
    ## Players:
    ## GameID,LeagueID,TeamID,PlayerID,AtBats,Runs,Hits,RBI,HomeRuns,Walks,Strikeouts,StolenBases,LeftOnBase,PitchingInnings,PitchingHits,PitchingRuns,PitchingEarnedRuns,PitchingWalks,PitchingStrikeouts,PitchingHomeRuns,CreatedDate,ModifiedDate
    ##
    ##
    ## Teams:
    ## 
    ## GameID,LeagueID,TeamID,Inning_1,Inning_2,Inning_3,Inning_4,Inning_5,Inning_6,Inning_7,Inning_8,Inning_9,Inning_10,Inning_11,Inning_12,Inning_13,Inning_14,Runs,Hits,Errors,CreatedDate,ModifiedDate
    ##  

    ##
    ##
    ## tables should look like
    ## 0 => home hitters
    ## 1 => away hitters
    ## 2 => home pitchers
    ## 3 => away pitchers
    ## 4 scoring  
    ##

    def process_baseball_stats(modData) 


      home_batters = modData[0].children[1]
      home_team_batting = modData[0].children[2]
      away_batters = modData[2].children[1]
      away_team_batting = modData[2].children[2]
      home_pitchers = modData[1].children[1]
      home_team_pitching = modData[1].children[2]
      away_pitchers = modData[3].children[2]
      away_team_pitching = modData[3].children[3]
      pitchers_schema = @espnSchemas['Pitchers']
      batters_schema = @espnSchemas['Batters']
      pitchers_trans = @trans['Pitchers']
      batters_trans = @trans['Batters']
      players = {}      
      teams = {}
    
      @csplitters = @splitters['Pitchers']
      home_pitchers_ = self.process_struct_of_data(pitchers_schema, home_pitchers, pitchers_trans, "player")
      away_pitchers_ = self.process_struct_of_data(pitchers_schema, away_pitchers, pitchers_trans, "player")
      home_pitchers_.each { |pitcher| 
          players = self.generate_player(players, pitcher, @home_team_id)
      }
      away_pitchers_.each { |pitcher| 
          players = self.generate_player(players, pitcher, @away_team_id)
      }

      @csplitters = @splitters['Batters']

      home_batters_ = self.process_struct_of_data(batters_schema, home_batters, batters_trans, "player")
      away_batters_ = self.process_struct_of_data(batters_schema, away_batters, batters_trans, "player")
      home_batters_.each { |batter|
          players = self.generate_player(players, batter, @home_team_id)
      }
      away_batters_.each { |batter|
          players = self.generate_player(players, batter, @away_team_id)
      }

      home_stats = {}
      away_stats = {}
      @csplitters = @splitters['Batters']
      home_batting = self.process_struct_of_data(batters_schema, home_team_batting, batters_trans, "team")
      home_batting.each { |k,v| 
          home_stats[k] = home_batting[k]
      }


      @csplitters = @splitters['Pitchers']
      home_pitching = self.process_struct_of_data(pitchers_schema, home_team_pitching, pitchers_trans, "team")
      home_pitching.each { |k,v | 
          home_stats[k] = home_pitching[k]
      }

      @csplitters = @splitters['Batters']
      away_batting = self.process_struct_of_data(batters_schema, away_team_batting, batters_trans, "team")

      away_batting.each { |k,v |
          away_stats[k] = v
      }
      @csplitters = @splitters['Pitchers']
      away_pitching = self.process_struct_of_data(pitchers_schema, away_team_pitching,pitchers_trans, "team")
      away_pitching.each { |k,v|
        away_stats[k] = v
      }
     

      team_stats = {}
      team_stats[@home_acc] = home_stats 
      team_stats[@away_acc] = away_stats

      ## now process the
      ## teams
      ##
    
      return {
        "teams" => team_stats,
        "players" => players
      }  
    end

    ## oputput of stats should
    ##
    ## resemble
    ## for players
    ##
    ## GameID,LeagueID,TeamID,PlayerID,ShotsAgainst,GoalsAgainst,Saves,Goals,Assists,Points,PenaltyMinutes,ShotsOnGoal,Blocks,Hits,Takeaways,Giveaways,FaceoffsWon,FaceoffsLost,FaceoffPercent,CreatedDate,ModifiedDate
    ##
    ## for teams
    ##
    ## GameID,LeagueID,TeamID,Period_1,Period_2,Period_3,Overtime_1,Overtime_2,Shootout,FinalScore,TotalShots,Shots_1,Shots_2,Shots_3,PowerPlays,PPConverted,PPPercent,PenaltyMinutes,FaceoffsWon,FaceoffPercent,Hits,Blocks,CreatedDate,ModifiedDate
    ##
    ##
    ##
    ## indice 4 and 5
    ## contain the needed
    ## data for players
    ##
    ## for teams we need to look
    ## in indice 9 and 10
    ##
    ## 9 => Shots On Goal
    ## 10 => power play summarry
    def process_hockey_stats(modData)
      players = {}
      teams = {}
      @csplitters = @splitters
      home_stats = modData[4].children[1]
      away_stats =  modData[5].children[1]
      sog_stats = modData[9].children[1]
      pps_stats = modData[10].children[1]
      goalies_home = modData[7].children[1]
      goalies_away = modData[8].children[1]
      players_schema = @espnSchemas['Players']
      goalies_schema = @espnSchemas['Goalies']
      trans_goalies = @trans['Goalies']
      trans_players = @trans['Players']
      home_stats_ = {}
      away_stats_ = {}
      struct = [
          "Total Shots",
          "PIM",
          "Hits",
          "Giveaways",
          "Takeaways",
          "Faceoffs won"
      ]
      data_to = {
          "Total Shots" => "Total",
          "PIM" => "PenaltyMinutes",
          "Hits" => "Hits",
          "Giveaways" => "Giveaways",
          "Takeaways" => "Takeaways",
          "Faceoffs won" => "FaceoffsWon"
      }
      
      stat_cmp = modData[1].children[1]
      cnt = 0
      struct.each {  |stat|
          res = stat_cmp.children[cnt].children[0].children[1].children[1].inner_html.match(/.*(\d)+/)
          home_stats_[data_to[stat]] = res[1].to_s
          cnt += 1
      }
      cnt = 0
      struct.each { |stat|
          res = stat_cmp.children[cnt].children[0].children[2].children[1].inner_html.match(/.*(\d+)$/)
          away_stats_[data_to[stat]] = res[1].to_s
          cnt += 1
      }


      goalies_home_ = self.process_struct_of_data(goalies_schema, goalies_home, trans_goalies, "player") 

      goalies_home_.each { |goalie|
          players = self.generate_player(players,goalie, @home_team_id)
      }
      goalies_away_ = self.process_struct_of_data(goalies_schema, goalies_home, trans_goalies, "player")
      goalies_away_.each { |goalie|
          players = self.generate_player(players,goalie, @away_team_id)
      }

      home_players = self.process_struct_of_data(players_schema, home_stats, trans_players, "player")
  
      home_players.each { |player|
          players = self.generate_player(players,player, @home_team_id)
      }
      away_players = self.process_struct_of_data(players_schema, away_stats,@trans, "player")
      away_players.each { |player| 
          players = self.generate_player(players,player, @away_team_id)
      }

      sog_home = sog_stats.children[0].xpath("td")
      sog_away = sog_stats.children[1].xpath("td")
      pps_home = pps_stats.children[0].xpath("td")
      pps_away = pps_stats.children[1].xpath("td")
      ## shots on goal stats for each period
      cnt = 0
      am = 4 
      sog_home.children.each { |stat|
          if cnt > 0 then
            if cnt == am then
              home_stats_['TotalShots'] = stat.to_s
            else
              home_stats_['Shots_' + cnt.to_s] = stat.to_s
              cnt += 1
            end
          end
          cnt += 1
      }

      cnt = 0
      sog_away.children.each { |stat|
        if cnt > 0 then
          if cnt == am then 
            away_stats_['TotalShots'] = stat.to_s
          else
            away_stats_['Shots_' + cnt.to_s] = stat.to_s
          end
        end
        cnt += 1
      }

        ## data  
        ## comes in like \d of \d

      imatches = pps_home.children[1].to_s.match(/(\d+)\s+of\s+(\d+)/)
      if imatches then
         home_pps_made = imatches[1]
         home_pps_attempted = imatches[2]
      end
      imatches = pps_away.children[1].to_s.match(/(\d+)\s+of\s+(\d+)/)

      if imatches then
        away_pps_made = imatches[1]
        away_pps_attempted = imatches[2]
      end


      home_stats_['PowerPlays'] = home_pps_attempted.to_f
      home_stats_['PPConverted'] = home_pps_made.to_f
      away_stats_['PowerPlays'] = away_pps_attempted.to_f
      away_stats_['PPConverted'] = away_pps_made.to_f
    
      home_stats_['PPPercent'] = ((home_pps_made.to_f / home_pps_attempted.to_f) * 100).to_s + "%"
      away_stats_['PPPercent'] = ((away_pps_made.to_f / away_pps_attempted.to_f) * 100).to_s + "%"

      teams[@home_acc] = home_stats_
      teams[@away_acc] = away_stats_
  
      #teams[@home_acc] = home_stats_ 
      ## for teams in hockey
      ## we need to look 
      ## at the data from players
      ##  
 
      return {
          "players" => players,
          "teams" => teams
      }

     
      ## get both 
      ## data for home
      ## and away

    end

    ## output of stats should resemble this
    ## structure:
    ##
    ##
    ## Teams:
    ## GameID,LeagueID,TeamID,Quarter_1,Quarter_2,Quarter_3,Quarter_4,Overtime_1,Overtime_2,FinalScore,FirstDowns,TotalYards,Turnovers,TotalPlays,RushingYards,TotalRushes,TotalPassingYards,Sacks,Interceptions,Punts,Penalties,PenaltyYards,Fumbles,FumblesLost,CreatedDate,ModifiedDate
    ##
    ## Players:
    ##
    ## 
    ##
    ## GameID,LeagueID,TeamID,PlayerID,PassingCompletions,PassingAttempts,PassingYards,PassingCompletionsPct,PassingSacks,PassingTDs,PassingInterceptions,PassingRating,PassingFumbles,RushingAttemps,RushingYards,RushingLong,RushingTDs,RushingFumbles,ReceivingCatches,ReceivingTargets,ReceivingYards,ReceivingLong,ReceivingTDs,ReceivingFumbles,KickingXPMade,KickingXPAttempts,KickingFGMade,KickingFGAttempts,KickingLong,KickingPoints,Punts,KickReturns,KickReturnYards,KickReturnLong,KickReturnTDs,PuntReturns,PuntReturnYards,PuntReturnLong,PuntReturnTDs,DefenseTackles,DefenseAssists,DefenseSacks,DefenseYardsLost,DefensePassesDefended,DefenseInterceptions,DefenseIntYards,DefenseIntTDs,CreatedDate,ModifiedDate
    ##
    ##
    ##
    ## modData is seperated as follows
    ## the first two containers we can 
    ## ignore
    ##
    ## format follows:
    ## home, away

    ##
    ##
    ## we're counting by 1
    ## please keep in mind implementation starts at 0

    ## 2, full team stats
    ## 3, -- passing
    ## 5,6 -- rushing
    ## 7,8 -- receiving
    ## 9,10 -- defensivu
    ## 10, 11 -- interceptions
    ## 12, 13 -- kick returns
    ## 14, 15  -- punt returns
    ## 16, 17 -- kicking 
    ## 18, 19 -- punting

    ##
    ## each modData should 
    ## also have its team stats directly below
    ## PLAYER_DATA
    ## TEAM_DATA
  
    def process_football_stats(modData)

      ## process passingf
      ##
      ## should look like:
      ##
      ## C/ATT YDS AVG TD  INT SACKS QBR RTG
      ## T. Brady  37/50 328 6.6 4 2 1-8 81.1  101.1
      ##
      ##  Team
      ## 
      teams = {}
      lookups = ['Passing', 'Rushing', 'Receiving', 'Defense', 'Interceptions', 'Punting', 'Kicking', 'Punt Returns', 'Kick Returns'] 


      ## treat NCF 
      ## and NFL different
      ## NCF does not show all stats
      ##

      full_team_stats = modData[1].children[1]
      if @league == "NFL" then
        schema = {
          "passing" => 1,
          "rushing" => 2,
          "receiving" => 3,
          "defense" => 4,
          "interceptions" => 5,
          "kick_returns" => 6,
          "punt_returns" =>7,
          "kicking" => 8,
          "punting" => 9
        }
      elsif @league == "NCF" then
        schema = {
           "passing" => 1,
           "rushing" => 2,
           "receving" => 4, 
            "interceptions" => 5,
            "kick_returns" => 6,
            "punt_returns" => 7,
            "kicking" => 8,
            "punting" =>9 
        } 
        
      end
       blobs = {}

        ## first value
        ## will always belong to stats table
        padding = 2
       schema.each { |k, v|
           
            v = v + padding 
            if v then 
              k1 = k.gsub(/_/, " ")
              new = ""
              first = k1.scan(/^(\w{1})([\w]+)|\s(\w{1})([\w]+)/).each { |m|

                  
                  if m[0] and m[1] then
                    new += m[0].capitalize + m[1]   
                  end
                  if m[2] and m[3] then
                    new += m[2].capitalize.to_s + m[3]
                  end
              }
              k1 = new

            blobs[k1] = {}
              if modData[v] then 
  
                
                blobs[k1]["home"] =  modData[v].children[1] 
                blobs[k1]["home_self"] =  modData[v].children[2]
                blobs[k1]["away"] =  modData[v + 1].children[1] 
                blobs[k1]["away_self"] =  modData[v + 1].children[2]
              end
            else
              ## set empty arrays when nothing is
              ## found
            end
        }

      ## blobs need to match lookups
      ## for both away and home
      ## so:
      #blobs['Passing'] => {home: blob for passing, away:blob for passing }
      players = {}
      teams = {}
      home_stats = {}
      away_stats = {}
      blobs.each { |lookup|
          lookup = "Passing"
         if blobs[lookup] then
           away =  blobs[lookup]['away']
           home = blobs[lookup]['home']

            puts "\n\n\n"
           home_self = blobs[lookup]['home_self']
           away_self = blobs[lookup]['away_self']
            @lookup = lookup
           if @splitters[lookup] then
            @csplitters = @splitters[lookup]
           else 
            @csplitters = {}
           end
   
           home_players = []
           away_players = []

           trans = @trans[lookup]

            if home and away then
             home_players = self.process_struct_of_data(@espnSchemas[lookup], home, trans, "player")
             home_players.each { |player|
                players = self.generate_player(players, player, @home_team_id)
             }
  
            away_players = self.process_struct_of_data(@espnSchemas[lookup], away, trans, "player")
            away_players.each { |player|
               players = self.generate_player(players, player, @away_team_id)
            }

            h_stats = self.process_struct_of_data(@espnSchemas[lookup], home_self, trans, "team")
              
            a_stats = self.process_struct_of_data(@espnSchemas[lookup], away_self, trans, "team")
            h_stats.each { |k, value|
              home_stats[k] = value
            }
            a_stats.each { |k, value| 
              away_stats[k] = value
            }
            end
         end



          teams[@home_acc] = home_stats
          teams[@away_acc] = away_stats
      }

      ## now get other team stats
      ## this is found in the 

      ## full data should resemble
      ## the following 
      ##
      ##
      ## Passing 1st down
      ## Rushing 1st down
      ## First down from penalty
      ## 3rd down 
      ##
      ## first column is start
      ## second is home
      ## third is away
      general = @trans['General']
        
      full_team_stats.children.each { |fs|
          team_v = fs.xpath("td")
          team_variate = team_v.first()

            
          ## heading data
          ## will not be nested
          ## while other data will be
          if not team_variate.class.to_s == 'Nokogiri::XML::Element' then
            if team_variate then
              stat = team_variate.inner_html
            else
              next
            end
          else 
           if team_variate then
            stat = team_variate.children[0].inner_html
           else
            next
           end
            
          end


          home_s = team_v[1].inner_html
          away_s = team_v[2].inner_html
          if general[stat] then 
            teams[@home_acc][general[stat]] = home_s
            teams[@away_acc][general[stat]] = away_s
          end
      }

    puts players

      ## return a 
      ## unified dataset

      return {
        "players" => players,
        "teams" => teams
      }
    end


    ## process the player info
    ## which should bereturning
    ##
    ## td>a[href='link']{text}
    ##
    def process_player_info(data)

      link = data.children[0].attr('href')
       anchor = data.children[0]
       name = anchor.inner_html
        ## process the id
       id = link.match(/id\/(\d+)/)
       id = id[1]
      
      info = {
        "id" => id,
        "url" => link,
        "name" => name
      }
      puts info
      return info
    end

    ## process the players info
    ## according to the anchor found
    ## this is ususlally the first
    ## element in stats
    ##
    
    ## compute on two scenarios
    ## when we're doing player
    ## we need to return the new
    ## player name with his stats
    ## when we're doing team just
    ## return the hash
    ##
    ## so 
    ## process_struct_of_data({STRUCT}, 'player') =>
    ##
    ## { 'name' => 'player_name', data => data }

    ## process_struct_of_data({STRUCT}, 'team')  =>
    ##  data 
    ##

    def process_struct_of_data(struct, mod, trans, for_)

    
      ## last row is always 
      ## team so we don't do this
      ## when we are processing 
      ## then pplayersj
      if for_ == "player" then
        data = [] 
        name = ''

        ## we may also have to treat
        ## singular contexes
        ## which are given by
        ## just one tr

        ## we will make a nodeset out of the
        ## elements
     
      
        if mod.node_name == "tbody" then
          ctx = mod.children
          ctx.each { |player| 
              cnt = 0 
              if not player.class.to_s == 'String' and not player.class.to_s == 'Array' then

                curdata = {} 
                stats = player.xpath("td")

                stats.each { |stat|
                  if cnt > 0 then 
                    curdata = process_data_further(cnt - 1, stat, struct, trans, curdata)
                  end

                  if cnt == 0 then
                    curdata = process_player_info(stat)
                  end

                  cnt += 1
                }

                curdata = self.process_percentages(curdata)
                data.push(curdata)
             end
            }
         else
            player = mod
            cnt = 0 

              if not player.class.to_s == 'String' and not player.class.to_s == 'Array' then

                curdata = {} 
                stats = player.xpath("td")
  
  
                stats.each { |stat|

                  ## reverse count back by one
                  ## as the schemas dont
                  ## need the first placeholder
                  if cnt > 0  then 
                    curdata = process_data_further(cnt  -1, stat, struct, trans, curdata)
                  end

                  ## process the player
                  ## info

                  if cnt == 0  then
                    curdata = process_player_info(stat)
                  end

                  cnt += 1
                }

                curdata = self.process_percentages(curdata)
                data.push(curdata)

             end

         end


        ## also get the percentages
      else
        ## start by 1 then first column always says "Team" we don't need it
        cnt = 0
        curdata = {}

        mod.children[0].children.each { |stat| 
          if  cnt >= 1 then
            curdata = process_data_further(cnt - 1, stat, struct, trans, curdata)
          end
          cnt += 1
        }

        ## process percventages
         
        curdata = self.process_percentages(curdata)
        data = curdata
      end

      return data
    end

    def process_percentages(curdata)

         @percentages.each { |k, percent| 

            if percent['type'] == 'data' then 
              data = percent['data']
              matcher = curdata[percent['matcher']].to_f
              sum = 0 
              data.each { |d|
                  sum += curdata[percent[d]].to_f
              }
              
              percent_rtg = (matcher / sum) * 100 
              curdata[k] = percent_rtg.to_s + "%"
            else
              upper = curdata[percent['upper']].to_f
              lower = curdata[percent['lower']].to_f
              percent_rtg = "0"
              if upper > 0 then
                percent_rtg = (lower / upper) * 100
                percent_rtg = String.new(percent_rtg.to_s)
              end

              curdata[k] = percent_rtg + "%"
            end
          }

        return curdata

    end


    ## take the set okf data 
    ## and process it according to schema rules
    ##
    def process_data_further(cnt, stat, struct, trans, curdata)
        
         if @csplitters[struct[cnt]] then
            if @csplitters[struct[cnt]].class.to_s == 'Hash' then 
                delimiter = @csplitters[struct[cnt]]['delimiter']
            else
                delimiter = "-"
            end

            check = @csplitters[struct[cnt]]['data']
     
            matches = stat.inner_html.match('(\d+)\\' + delimiter + '(\d+)')

            if matches then
              first = check[0]
              second = check[1]
              curdata[first] = matches[1]
              curdata[second] = matches[2]
            end

         else
            if trans[struct[cnt]] then
              curdata[trans[struct[cnt]]] = stat.inner_html
            end
         end


        puts curdata

      return curdata
    end


    ## forms the ESPN url, where 
    ## we need a secion and league
    ## and gameId
    ##
    def form_url(area, gameId)
        url = String.new(@match_url)
        url = url.gsub(/\{endpoint\}/, area)
        url = url.gsub(/\{league\}/, @league.downcase)
         
        if @leagueFriendlyName == "Soccer" then
          return url + gameId + "/statistics.html"
        end
        
        return url + gameId
    end

    def get_nascar_id(cup_name)
      id = ""
      alpha = ('a' .. 'z');

      name = cup_name.match(/([A-Za-z]+)$/)[1]

      name.split("").each { |w|
        cnt = 0
        alpha.each { |a|  
            if a  == w then
              id += cnt.to_s 
            end
            cnt += 1
        }
      }

     return id.to_s
      
    end
    ## look at the match
    ## and retrieve all information
    ##
    ## this should be relative to
    ## the sport and league
    
    ## start with the scoreboard
    ## look at the following 
    ##
    ## scoreboard,
    ## stats
    ## boxscore
    ## inProgress map
    ## 1 => inProgress
    ## -1 => not begun
    ## 0 => over
    def parse_match(gameId)
      
       inProgress = 1
       finalScore = 0

       pureGameId = gameId
        
       ## our game id becomes the 
       ## only the digits starting
       ## at the game
       ##
       ##
       ##
       ## nascar ids need to turn
       ## their cups into their numeric form so
    
       if @league == "NASCAR" then
          #gameId = self.get_nascar_id(gameId)

          gameId = pureGameId.to_s.gsub(/(\D+)/, "")
       end

       game = self.get_game_if_exists(gameId)

     
       if game then
        if game['InProgress'] == 0 then
          print "This game ended and we stored its results.."
          return
        end
       end

       url = self.form_url("boxscore", pureGameId)
       puts "trying url: "
       puts url

       ## for testing
       #resp = @client.get(url + gameId)

       resp = @client.get(url)

       parser = @parser = resp.parser
       gametitle = parser.xpath("//title").first.inner_html.match(/([^^]+)\-/)
       ## match until
       ##  we have a hyphen
       ##
       ## so 
       ## Team A v  Team B - ESPN => Team A v Team B
       if gametitle then
        gametitle = gametitle[1].gsub(/\s?\-.*/, "")
       else
        gametitle = parser.xpath("//title").first.inner_html

       end
       


       ## get the attendance
       ##
       ## best way is to match 
       ## what is provided
       ## as Attendance:\s\s\s\+(\d)
        attendance = 0
        matches = parser.inner_html.match(/Attendance\:[A-Za-z<>\\\/\s]+([\d]+,?[\d]+)/)
        if matches then
          attendance = matches[1].gsub(/,/, "")
        end
        


       ## get the start time of the game
       ## this is available in game-time-location
       ## we need the first children
       ## this will only work on some leagues, through
       ## this class others need there own
       ##
       ## dates look like
       ## H:i, M D, Y
       ##
       startDate = ''
       time_of_match =  parser.xpath("//div[@class='game-time-location']").first()
       months = {
          "January" => 1,
          "February" => 2,
          "March" => 3,
          "April" => 4,
          "May" => 5,
          "June" => 6,
          "July" => 7,
          "August" => 8,
          "September" => 9,
          "October" => 10,
          "November" => 11,
          "December" => 12
       }
       if time_of_match then
         puts time_of_match
         gameTime = time_of_match.children[0].inner_html
         #startDate = Date.parse(gameTime).strftime("%Y-%M-%d  %H:%i:%s")
        
         ## matches 
         ## 
         ## 2:00 PM, April 5, 2015
         ##
         splitter = gameTime.match(/([\w\d]+):([\d]+).*(AM|PM).*,\s?+([\w]+)\s+?([\d]+),\s+?([\d]+)/)
         startDateHour = splitter[1].to_i
         startDateMinutes = splitter[2].to_i 
         startDateAmOrPm = splitter[3]
         if startDateAmOrPm == "PM" then
          startDateHour += 12
         end

         startDateMonth = months[splitter[4]].to_i
         startDateDate = splitter[5].to_i
         startDateYear = splitter[6].to_i
         ## we don't get seconds however
         ## put to 0 as it can effect the inputted minutes
         startDate = DateTime.new(startDateYear, startDateMonth, startDateDate, startDateHour, startDateMinutes, 0, '+7').strftime("%Y-%m-%d %H:%M:%S")

       else
         ## match other leagues
         ## here

         if @league == "PGA" then
          ## PGA gives us:
          ## Month start-end, year
          ##
          ## like:
          ##
          ## March 3-4, 2015
          split = parser.xpath("//*[@class='date']").first()
          if split then 
            split = split.inner_html
            splitter = split.match(/([A-Za-z]+)\s?([\d]{1,2})\-([\d]{1,2}),\s+?([\d]{4})/)
            startDateYear = splitter[4].to_i
            startDateMonth = months[splitter[1]].to_i
            startDateDate = splitter[2].to_i 
            startDate = DateTime.new(startDateYear, startDateMonth, startDateDate, 0,0,0, '+7').strftime("%Y-%m-%d %H:%M:%S")
          end

         elsif @league == "NASCAR" then
           ## NASCAR gives us
           ## the date/time before we get to the match
           ## so to_traverse_times

           startDate = @to_traverse_times[pureGameId]
           
         elsif @league == "MLS" then

          ## MLS dates look like, 
          ## date/month/year time
          ##
          ## UK format
        
          #split = parser.xpath("//div[@class='match-details']")
          #puts split
          #split =split.children[2].inner_html 
          splitter = parser.inner_html.match(/var\s+?d\s+\=\s+new\s+Date\(([\d]+)\)/)
          
          unix_time = splitter[1].to_i / 1000
          puts unix_time

          startDate = Time.at(unix_time).strftime("%Y-%m-%d %H:%M:%S")
        
         end

       end
    


       ## the priminitive check
       ## is whether this game is current, has been finished
       ## or is in the future
       ##
       ## we do this by checking the 
       ## if our tickets button exists
       ## when it does the game has not started
      

       status = parser.xpath("//*[@class='title']")

      not_started = false
      ended = false
      status.each { |st|
          if st.inner_html == "Tickets" then
            not_started = true
          end
          if st.inner_html == "Recap" then
            ended = true
          end
      }

      if not_started then
        inProgress = -1
        game = {
           "gameId" => gameId, 
            "title" => gametitle,
            "InProgress" => inProgress
         }
         return self.insert_or_update_game(game)
      end


      home_final_score = 0
      away_final_score = 0
      ## once we have recap
      ## we can stop 
      ## processing this match
      ## we will do one 
      ## last round for quality
      if ended then
        inProgress = 0


        if @leagueFriendlyName == "Baseball" or self.is_singular_league() then 
          ## TODO does not support
          ## the class 'ts'
          ## find a workaround
          home_final_score = 0   
          away_final_score = 0
        else
          ## we need to get the final
          ## score foreach team now
          matches = parser.xpath("//td[contains(@class,'ts')]")
          puts matches
          home_final_score = matches[0].inner_html
          away_final_score = matches[1].inner_html
        end
      end

      
      title =  parser.xpath("//title").first.inner_html


      ## get the league
      ## id if we dont already have it
      ##
      ## THIS will be deprecated..
      ##
      unless @leagueId

        #if not  @leagueFriendlyName ==  "Golf"  then
        #  @leagueId = parser.inner_html.match(/sportId:\s?(\d)+/)
        #  @leagueId = @leagueId[1]
        #  self.check_league(@leagueId)
        #end
      end

      ## todo also needs a way to chekc whether
      ## the game is over

       #matchup = parser.xpath("//*[@id='matchup-" + @league.downcase +  "-" + gameId +"']")

      if not self.is_singular_league()

       if not @leagueFriendlyName == 'Soccer' then
         matchup = parser.xpath("//*[@id='matchup-" + @league.downcase +  "-" + gameId +"']")
         away = matchup.xpath("//*[@class='team away']")
         home = matchup.xpath("//*[@class='team home']")


          if away.children[1].children[0].children.length > 3 then
            away_info = away.children[1].children[0].children[2]
          else
            away_info = away.children[1].children[0].children[0]
          end

          if home.children[1].children[0].children.length > 3 then
            home_info = home.children[1].children[0].children[2]
          else
            home_info = home.children[1].children[0].children[0]
          end
            
          puts away_info
          puts home_info

         away_name = away_info.inner_html
         home_name = home_info.inner_html
         away_team_url = away_info.attr("href")
         home_team_url = home_info.attr("href")

         ## now we can pass the team links
         ## to get_team_info
         ## which should successfully get the info
         ## should our info be right

         ## getting the accronymes
         ## for the team should
         ## follow this approach
         ## /team/_/name/ACCRONYM/
         ##
         ## TODO chekc on new update
         matches = away_team_url.match(/\/([\w\-]+)\/?/)
         @away_acc = matches[1].upcase
         matches1 = home_team_url.match(/\/([\w\-]+)\/?/)
         @home_acc = matches1[1].upcase
         @home_team_id = self.get_team_id(home_team_url)
         @away_team_id = self.get_team_id(away_team_url)
        
         puts "Processing: " + @home_acc + " vs. " + @away_acc
         ## our two next siblings are
         ## the quarter points for the first and second
         ## team

         els = parser.xpath("//*[@class='linescore']")

         
         ## get the total scores

         totals = els.xpath("//td[contains(@class,'ts')]")
     

         if not @leagueFriendlyName == "Baseball"  then
           home_score = totals[0].inner_html
           away_score = totals[1].inner_html
         end


         scores = els.children[2].xpath("//*[contains(@style, 'text-align:center')]")
         scores_full = Array.new 
         start = false 
         cnt = 0
         scores.each { |score|
            score = String.new(score.inner_html.gsub(/\s+/, ""))
            if score ==  "T" or score == "9"
              start = true
              next
            end

            if not is_number(score) and not score  == "-" and scores_full.length > 0 then
              break
            end

            if start 
              if is_number(score) then
               scores_full.push(score) 
              end
              if score == "-" then
               scores_full.push(0)
              end
            end

            cnt += 1
         }


        
         home_scores = Array.new
         away_scores = Array.new
         team_scores = scores_full.length / 2
         home_scores = scores_full.slice(0, team_scores)
         away_scores = scores_full.slice(team_scores, scores_full.length)

         puts "Home Scores are: "
         puts home_scores
         puts "Away scores are: "
         puts away_scores
       else

          ##processing
          ##
          ## soccer scores is a little
          ## different we need to look at the class
          ## score take out hyphens
          ##
          ##
        
          score = parser.xpath("//div[@class='score-time']").first()

          score = score.children[1].children[1].inner_html
          scores = score.match(/(\d+)\s+-\s+(\d+)/)
          puts scores
          home_scores = []
          away_scores = []
          home_score = scores[1]
          away_score = scores[2]

          ## get the home
          ## acc as well as away
          ##
          ##
          ##
          ##

          away_team = parser.xpath("//div[@class='team away']").first()
          home_team = parser.xpath("//div[@class='team home']").first()
          aw_s = away_team.children[1]
          hm_s = home_team.children[1]
          @home_acc = home_team.children[3].children[1].inner_html
          @away_acc = away_team.children[3].children[1].inner_html
          away_link = @entrypoint['BaseURL'] + aw_s.attr("href")
          home_link = @entrypoint['BaseURL']+ hm_s.attr("href")
          @home_team_id = home_link.match(/(\d+)\/index/)[1]
          @away_team_id = away_link.match(/(\d+)\/index/)[1]
       end
     end

  
      ##  todo
      ## needs revisioning
      if not self.is_singular_league()
        if @leagueFriendlyName == "Soccer" then
          mod_data = parser.xpath("//section[contains(@class, 'mod-container')]")
        else 
    			 mod_data = parser.xpath("//table[contains(@class,'mod-data')]")
        end
     else
        mod_data = parser.xpath("//div[contains(@class,'mod-content')]")
     end
      ## hooks we now need
      ## to look at how
      ## we are going to process
      ## the next data structs
      ##
      ## Basketball written  03/25/2015
      ## Football written  03 /26/2015
      ## Hockey written 03/27/2015
      ## Baseball written 03/28/2015
      if @leagueFriendlyName == "Basketball" then 
          stats = process_basketball_stats(mod_data)
      end  
 
      if @leagueFriendlyName == "Football" then
          stats = process_football_stats(mod_data)
      end

      if @leagueFriendlyName == "Hockey" then
          stats = process_hockey_stats(mod_data)
      end
      if @leagueFriendlyName == "Baseball" then
          stats = process_baseball_stats(mod_data)
      end
      if @leagueFriendlyName == "Golf" then
          stats = process_golf_stats(mod_data)
      end
      if @leagueFriendlyName == "Racing" then
        stats = process_racing_stats(mod_data)
      end
      if @leagueFriendlyName == "Soccer" then
        stats = process_soccer_stats(mod_data)
      end


      players = stats['players']
      team_stats = stats['teams']
      teams = {
        "away" => {
          "name" => @away_acc,
           "prefix" => @away_acc,
           "url" => away_team_url,
           "id" => @away_team_id,
            "scores" => away_scores,
           "finalScore" => "",
           "stats" => team_stats[@away_acc]
        },
       "home" => {
          "name" => @home_acc,
          "url" => home_team_url,
          "prefix" => @home_acc,
          "id" => @home_team_id,
          "scores" => home_scores,
          "finalScore" => home_final_score,
          "stats" => team_stats[@home_acc]
       }
      }
      game = {
        "ESPNUrl" => url,
        "gameId" => gameId,
        "GameTitle" => gametitle,
        "Attendance" => attendance,
        "FinalScore" => finalScore,
        "LeagueID" => @leagueId,
        "HomeTeamId" => @home_team_id,
        "AwayTeamId" => @away_team_id,
        "InProgress" => inProgress.to_s,
        "StartDate" => startDate,
        "players" => players,
        "teams" => teams
      }

      @currentGame = game
      self.insert_or_update_game(game)
    end


	  ## check whether
		## a league
		## exists
		## this should be ran before
		## a game check
		def league_id_exists(leagueId)
			q = @db.query("SELECT * FROM `Leagues` WHERE LeagueID = '" + leagueId + "'")
			return self.eval_count(q)
		end

		## insert a league into the database
		## t
		def insert_league(leagueId)
			str = @dbsyntax.insert_str(@db, "Leagues", {
				"LeagueID" => @leagueId,
				"LeagueName" => @entrypoint['LeagueName'],
				"SportName" => @entrypoint['FriendlyName']
			})
			@db.query(str)
		end

		def check_league(leagueId) 
			if not league_id_exists(leagueId) then
				insert_league(leagueId)
			end
		end

    def insert_or_update_game(game)
     if not self.game_id_exists(game['gameId']) then
      return self.insert_game(game)
     else
      return self.update_game(game)
     end
    end

    ## check whether  
    ## we have already
    ## stored data for this
    ## or not
    def game_id_exists(gameId)
      if gameId then
        q = @db.query("SELECT * FROM `Games` where GameId = '" + gameId + "'")
        return self.eval_count(q)       
      end


      return -1
    end

    ## update a game needs
    ## the game hash object
    ## @param game:
    ## game->teams
    ## game->players
    ##
   ## on success this will insert
    ## data in the following tables
    ## league.games
    ## league.teams
    ## league.players
    def update_game(game)

      modifiedDate = self.make_time() ##time.strftime('%Y-%m-%d')
      updateStr = @dbsyntax.update_str(@db, "Games", "GameID", game['gameId'], {
        "InProgress" => game['InProgress'],
        "ModifiedDate" => modifiedDate
      })

      @db.query(updateStr)
      ## insert team stats


      if not game['InProgress'] == -1 then
        if not self.is_singular_league() then
          self.insert_or_update_team(game['gameId'], game['teams']['home'])
          self.insert_or_update_team(game['gameId'], game['teams']['away'])
        end

        game['players'].each { |k, player|
           self.insert_or_update_player(game['gameId'], player)
        }
      end
    end

    def player_exists(playerId)
      if playerId then
       q =  @db.query("SELECT * FROM `Players` WHERE PlayerId = '" + playerId + "';")

       return eval_count(q)
      end
      return -1 
    end

    def eval_count(rows)
      if rows.num_rows > 0 then
        return true
      end
      return false
    end
    
    def team_exists(teamId)
    
      if teamId then
        q = @db.query("SELECT * FROM `Teams` WHERE TeamID = '" + teamId + "';") 
        return self.eval_count(q) 
      end
     
      return -1
    end

    def game_player_exists(gameId, playerId) 
     if playerId then
       q = @db.query("SELECT * FROM `" + self.get_game_player_table() + "` WHERE PlayerId = '" + playerId + "' AND GameId = '" + gameId + "'")

       return self.eval_count(q)
     end
      return -1
    end

    def game_team_exists(gameId, teamId)
      if teamId then
        q = @db.query("SELECT * FROM `" + self.get_game_team_table() +  "` WHERE  TeamID = '" + teamId + "' AND GameID = '" + gameId + "'")
          
        return self.eval_count(q)
      end

      return -1
    end

    ## needs
    #implementation
    ## using the fields
    def insert_player(player)
      createdDate = self.make_time() ##time.strftime("%Y-%m-%d")
      modifiedDate = createdDate
      
      q = @dbsyntax.insert_str(@db, self.get_players_table(), {
        "PlayerID" => player['id'],
        "PlayerName" => player['name'],
        "ESPNURL" => player['url'],
        "LeagueID" =>  @leagueId,
        "TeamID" => player['teamId'],
        "CreatedDate" =>  createdDate,
        "ModifiedDate" => modifiedDate
      })
      @db.query(q)
    end

   
    ## checks whether the league 
    ## is a singular or team based
    def is_singular_league()
      if @leagueFriendlyName == "Golf" or @leagueFriendlyName == "Racing" then
        return true
      end

      return false
    end


    def insert_game_player(gameId, player) 
      data = self.get_league_player_schema(player)

      q = @dbsyntax.insert_str(@db, self.get_game_player_table(), data)

      return @db.query(q)
    end

    def update_player(player)
      modifiedDate = self.make_time() ##Time.new().strftime("%Y-%m-%d")
      q = @dbsyntax.update_str(@db, self.get_players_table(), "PlayerID", data['id'], {
        "ModifiedDate" => modifiedDate
      })

      return @db.query(q)
    end

    def update_game_player(gameId, player)

      data = self.get_league_player_schema(player, true)
      q = @dbsyntax.update_str(@db, self.get_game_player_table(), "GameID", gameId, data)
			return @db.query(q)
    end

    def insert_team(team)

      createdDate = self.make_time() #time.strftime("%Y-%m-%d")
      modifiedDate = createdDate
      q = @dbsyntax.insert_str(@db, self.get_teams_table(), {
           "TeamId" => team['id'],
           "TeamPrefix" => team['prefix'],
           "ESPNUrl" => team['url'],
           "LeagueID" => @leagueId,
           "createdDate" => createdDate, 
           "modifiedDate" => modifiedDate
      })
      return @db.query(q)
    end

    def update_team(team)
      modifiedDate = self.make_time() # time.strftime("%Y-%m-%d")
      q = @dbsyntax.update_str(@db, self.get_teams_table(), {
        "TeamID"  => team['id'],
        "TeamPrefix" => team['prefix'],
        "LeagueID" => @leagueId,
        "ESPNUrl" => team['url'],
        "ModifiedDate "=> modifiedDate
      })
      return @db.query(q)
    end

    def get_game_team_table()
      return "TeamStats_" + @leagueFriendlyName
    end

    def get_teams_table()
      return "Teams"
    end

    def get_players_table()
      return "Players"  
    end
  
    def get_game_player_table()
      return "PlayerStats_" + @leagueFriendlyName
    end

    def get_game_team_table()
      return "TeamStats_" + @leagueFriendlyName
    end

    ## switch schemas 
    ## according to 
    ## the league 
    ## param 
    ## @param data
    ## set of parameters
    ## that satify this schema
    ## things that are always
    ## there should be
    ## CreatedDate, ModifiedDate
    def get_league_player_schema(data, isUpdate=false)
      pred = {}

      ## player schemas
      ## for matches
      ## are usually stat based

       @playerSchema.each { |k|

          if data[k] then
            pred[k] = data[k]
          end
       } 
      time = Time.new 
      if not isUpdate then
        createdDate =  time.strftime("%Y-%m-%d")
        pred['CreatedDate'] =  createdDate
        pred['PlayerID'] =  data['id']
      end

      modifiedDate = time.strftime("%Y-%m-%d")
      ## we also
      ## need game id, league id and team id
      ## createdDate and modified date
      pred['GameId'] = @currentGame['gameId'] 
      if not  self.is_singular_league() then
        pred['TeamId'] = data['teamId']
      end
      pred['LeagueId'] = @leagueId
      pred['CreatedDate'] = createdDate
      pred['ModifiedDate'] = modifiedDate

      return pred
	  end

    ## switch schemas 
    ## according
    ## @data => set of parameters that satisfy
    ## this schema
    ##
    ##
    ## @params data will be a teamblob
    ## scores in it can be traversed until either
    ## 0 or nil
    ## is met this tells us which Quarter we're in
    def get_league_team_schema(data, isUpdate=false)

      pred = {}
      lang = @scorePeriods
      cnt = 0
      ## keep ## traversing the 
      ### score period
      ## set until 
      ## scores
      ## is unavalilable
      lang.each { |l|

        pred[l] = data['scores'][cnt]

        cnt += 1
     }

     ## final score 
     ## is always available
     ## on all. so is half and half 2
      
     ## we only evaluate this 
     ## need currentGame 
     ## variable
     if @currentGame['inProgress'] = 0  and not @leagueFriendlyName == "Baseball" then
      ## we can process this
      #score now
      pred['FinalScore']  = @currentGame['FinalScore']
     end


     @teamSchema.each { |field|

        ## check for existance
        ## here

        if data[field] then
          pred[field] = data[field]
        end
      }

			## other things
			## that we will need
			## can be found
			## in main objects and current game
      
			date = self.make_time() ##time.strftime("%Y-%m-%d")	
      if not isUpdate then
        pred['CreatedDate'] = date 
      end

			pred['GameID'] = @currentGame['gameId']
			pred['LeagueID'] = @leagueId

      if not @leagueFriendlyName == "Baseball" then
        pred['FinalScore'] = data['finalScore']
      end

			pred['TeamID'] = data['id']
			pred['ModifiedDate'] = date
    
      return pred
    end

    ## insert the 
    ## game stats for
    ## this team
    ## we can find
    ## this information
    ## in the stats field we
    ## also need to match
    ## our schema with that
    ## of the League
    def insert_game_team(gameId, team)
      data = self.get_league_team_schema(team)

      q = @dbsyntax.insert_str(@db, self.get_game_team_table(), data)

      @db.query(q)
    end

    def update_game_team(gameId, team)
      data = self.get_league_team_schema(team)
      q = @dbsyntax.update_str(@db, self.get_game_team_table(), "GameID", gameId, data)

      @db.query(q)
    end

    ## these need to insert
    ## or update players
    ## based on their
    ##
    ## 
    ## player ids need to know
    ## there team id
    def insert_or_update_player(gameId, player)
      pReturn = self.player_exists(player['id']) 
      if pReturn then
        if self.game_player_exists(gameId, player['id'])
          self.update_game_player(gameId, player)
        else

          self.insert_game_player(gameId, player)
        end
      else  

        if pReturn == -1 then
          ## undetermined
          ## behavoir we need
          ## the program's
          ## player id
        else 
          ## a zero return means
          ## we can add when
          ##
          self.insert_player(player) 
          self.insert_game_player(gameId, player)
        end
      end
    end

    ## these need to update
    ## the teams based on their
    ## team ids
    def insert_or_update_team(gameId, team)
      teamRet = self.team_exists(team['id'])

      if teamRet then
        if self.game_team_exists(gameId, team['id']) then
          self.update_game_team(gameId, team)
        else
          self.insert_game_team(gameId, team) 
        end
      else 
        if teamRet == -1 then
          ## undefined
          ##
        else
          self.insert_team(team)
          self.insert_game_team(gameId, team)
        end
      end  
    end

    ## same as above
    ## needs the game
    ## hash object
    def insert_game(game)
       time = Time.new
       
       startDate = self.make_time() #time.strftime("%Y-%m-%d")
       modifiedDate = startDate
       createdDate = startDate
   
       
       insertStr = @dbsyntax.insert_str(@db, "Games", {
        "GameId" => game['gameId'],
        "InProgress" => game['InProgress'],
        "ESPNUrl" => game['ESPNUrl'],
        "HomeTeamId" => game['HomeTeamId'],
        "AwayTeamId" => game['AwayTeamId'],
        "Attendance" => game['Attendance'],
        "GameTitle" => game['GameTitle'],
        "ModifiedDate" => modifiedDate,
        "CreatedDate" => createdDate,
        "StartDate" => game['StartDate'] 
       })
       @db.query(insertStr)
       ##insert the team stats
       if not game['InProgress'] == -1 then

         if not self.is_singular_league() then
           self.insert_or_update_team(game['gameId'], game['teams']['home'])
           self.insert_or_update_team(game['gameId'], game['teams']['away'])
         end

         ## insert the player stats 

         game['players'].each { |k, player|
          self.insert_or_update_player(game['gameId'], player)
         }
       end
    end
    ## start the scraper
    ## this should on success
    ##  look at all the matches
    ## for this league and 
    ## return the results
    ## accordingly
    def start 
       url = @entrypoint['url'] 
       puts "Starting scan.."
       puts "On:"
       puts url
       result = @client.get(url)
       parser = result.parser


      if self.is_singular_league() then


        if @leagueFriendlyName == "Golf"
          ## for golf we need to process
          ## the tournament ids which
          ## is found in a select box
          els = ""
          tournaments = parser.xpath("//select[@id='all-tournaments']")
          tournaments.children.each { |tournament|
              ## ignore the default select value
              if not tournament.attr("value") == "-1" then
                @to_traverse.push(tournament.attr("value"))
              end

          }
        end


        if @leagueFriendlyName == "Racing"
          ## for racing we need
          ## similar checks
          ## to golf only
          ## in a table struct


          tournaments = parser.xpath("//div[@class='mod-content']")

          
          ## rows are odd and even we need both
          odds = parser.xpath("//tr[contains(@class,'odd')]")
          evens = parser.xpath("//tr[contains(@class,'even')]")
          all = evens + odds
          ##logic follows  
          ## some of these
          ## matches may be in pending
          ## state so for these
          ## we  can skip
          ## the ones that are
          ## in progress we will
          ## scan
          all.each { |race|
            ## first column 
            ##  contains the date time
            ## for nascar it is needed 
            time = race.children[0].inner_html.gsub(/\<br\>/, " ")
            ## datetime should be
            ## 3 letter date,  3 letter month, year
            dates = {
              "Sun" => 1,
              "Mon" => 2, 
              "Tue" => 3, 
              "Wed" => 4,
              "Thu" => 5,
              "Fri" => 6,
              "Sat" => 7
            }
            months = {
              "Jan" => 1,
              "Feb" => 2,
              "Mar" => 3,    
              "Apr" => 4,
              "May" => 5,
              "Jun" => 6,
              "Jul" => 7,
              "Aug" => 8,
              "Sep" => 9,
              "Oct" => 10,
              "Nov" => 11,
              "Dec" => 12
            }
            
            y = Time.new().strftime("%Y").to_i  
            splitter = time.match(/(\w{3}),[^^]{1}?+(\w{3})[^^]{1}+([\d]+):([\d]+)\s+?(AM|PM)/)
            month = months[splitter[2]].to_i
            date = dates[splitter[1]].to_i
            hour = splitter[3].to_i
            minutes = splitter[4].to_i
            am_or_pm = splitter[5]
            if am_or_pm == "PM" then
              hour += 12
            end
            ## where year is always the current year
            ##
            dt = DateTime.new(y, month, date, hour, minutes, 0, '-7').strftime("%Y-%M-%d %H:%M:%S")

            els = race.children[3]
            els.children.each { |schema|
              if not schema.class.to_s == "Nokogiri::XML::Element" then
                next
              end

              
              if schema then
                if schema.inner_html ==  "Race Results" then
      
                  ## link will look like 
                  ## http://espn.go.com/racing/raceresults?raceId=201502210306&series=xfinity
                  ## we need to match everything after the raceId
                  ##
                  ## when the series is not provides we get an error
                  ## so it neeeds to later take care of the series
                  ## parse_match
                  link = schema.attr("href").match(/raceId=(.*)$/)
                  @to_traverse.push(link[1])
                  @to_traverse_times[link[1]] = dt
                end
                if schema.inner_html == "Starting Grid" then
                  ## starting grid
                end
                if schema.inner_html == "Tickets" then
                  ## we don't want to process
                  ## this as it is not in progress
                  ## yet so we will skip
                  ##
                  ##
                  next

                end
              end
            }
          } 
        end

        
      else

        if @leagueFriendlyName == "Soccer" then

          els = parser.xpath("//div[@class='score-box']") 
          
          els.each { |el|
              game = el.xpath("//div[@class='score full']").first()
 
              m = game.attr("data-gameid")
              @to_traverse.push(m)
          }    
        else

          ## consider
          ## taking ESPN's
          ## JSON structure
          ## and working with this
          #els = parser.xpath("//div[@id='data-scoreboard']").first()
        
          #obj = JSON.parse(els.attr("data-data"))


          if self.is_college_league() then 

            ## follow gameLink
            ## pattern for college
            ## leagues
            ##
            links = parser.xpath("//div[contains(@id,'gameLinks')]")
            links.each { |link|
              ## need to 
              ##
              ##
              _id = link.attr("id").match(/(\d+)/)
              @to_traverse.push(
                _id[1]
              ) 
            }

          else
            gameIds = parser.inner_html.scan(/\?gameId=(\d+)/).each { |gId|
                if not  @to_traverse.include? gId[0] and is_number(gId[0]) then
                  @to_traverse.push(gId[0])
                end
            }
      
          end


        end
           
      end

      @to_traverse.each { |id|
         self.parse_match(id)
      }
   
    end


    def get_league_table()

    end
    ## adds a league
    ## to the database
    ##
    def add_league(opts)

      
      str = @dbsyntax.insert_str(@db, self.get_league_table(), opts) 
      
      @db.query(str)

    end


    def is_college_league()
      if @league == "NCF" or @league == "NCW" or @league == "NCB" then
       return true
      end
      return false
    end

    ## need to return Y-m-D H:i:S
    def make_time()
      time = Time.new()

      return time.strftime("%Y-%m-%d %H:%I:%S")
    end


    ## update leagues
    ## 
    ## this will update
    ##  the league in 
    ## database
    def update_league(opts)
      str = @dbsyntax.update_str(@league_table, opts)

      @db.query(str)
    end

    ##
    ##
    def update_match(opts)
      str = @dbsyntax.update_str(@db, @league_table,opts)
      @db.query(str)
    end

    def add_match
      str = @dbsyntax.insert_str(@db, @league_table, opts)
      @db.query(str)
    end
end


## examples you can use as follows
##
# scr = SportsScraper.new("NASCAR")
# scr.start()
##
##
##
