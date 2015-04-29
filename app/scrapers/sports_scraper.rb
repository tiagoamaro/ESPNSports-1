require "mysql"
require "nokogiri"
require "mechanize"

#-----------------------------------------------------------------------------------------------
def is_number(number)
  true if Float(number) rescue false
end

#-----------------------------------------------------------------------------------------------
class DBSyntax

#-----------------------------------------------------------------------------------------------
def get_schema(db, table)

    sql = "EXPLAIN " + table
    rows = db.query(sql)
    rows_ = Array.new

    while row = rows.fetch_row
          rows_.push(row)
    end

    return rows_
end

#-----------------------------------------------------------------------------------------------
def foreign_key_checks_off(db)
    db.query("SET FOREIGN_KEY_CHECKS=0;")
end

#-----------------------------------------------------------------------------------------------
def escape_val(val)
    return val.gsub(/'/, "\\'")
end

#-----------------------------------------------------------------------------------------------
def insert_str(db, table, opts)
    db.query("SET FOREIGN_KEY_CHECKS=0;")

    str = String.new("")
    str += "INSERT INTO `" +  table + "` ("

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
    #puts str
    return str
end

#-----------------------------------------------------------------------------------------------
def update_str(db, table, key, value, opts)
    self.foreign_key_checks_off(db)

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

    return str
end

#-----------------------------------------------------------------------------------------------
def update_str_with_conditionals(db, table, conditions = {}, data)
    self.foreign_key_checks_off(db)

    str = String.new("")

    str += "UPDATE `" + table + "` SET "

    data.each do |k, v|
         if not v then
            v = ""
         end
         if k and v then
            str += "`" + String.new(k.to_s) + "`" + " = '" + String.new(v.to_s) + "', "
         end
    end

    str = str.gsub(/,\s$/,"")

    unless conditions.empty?
        where_conditions = conditions.map { |key, value| "`#{key}` = '#{self.escape_val(value.to_s)}'" }
        where_conditions = where_conditions.join(' AND ')

        str += " WHERE #{where_conditions}"
    end

    return str
end

#-----------------------------------------------------------------------------------------------
end

#-----------------------------------------------------------------------------------------------
class SportsScraper

#-----------------------------------------------------------------------------------------------
def initialize(league, task_logger, scrape_date)
    @entrypoints = {}

    @datestr = self.make_time(scrape_date)
    puts "----------------------------------------------------------------------------"
    puts "Current Date: #{@datestr}"
    @task_logger = task_logger

    @inheritors = {
        "NBA" => ['WNBA', 'NCB', 'NCW'],
        "NFL" => ['NCF']
    }

    @entrypoints['NBA'] = {
        "url" =>  "http://scores.espn.go.com/nba/scoreboard?date=" + @datestr,
        "LeagueID" => 10,
        "LeagueName" => "NBA",
        "FriendlyName" => "Basketball",
        "league_table" => "Players_NBA",
        "players_table" => "Players_NBA",
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
        "espnTeamSchema" => [
            ["FGM-A", "Splitter"], # This is mapped on @entrypoint's splitters
            ["3PM-A", "Splitter"], # This is mapped on @entrypoint's splitters
            ["FTM-A", "Splitter"], # This is mapped on @entrypoint's splitters
            ["OREB", "OffRebounds"],
            ["DREB", "DefRebounds"],
            ["REB", "Rebounds"],
            ["AST", "Assists"],
            ["STL", "Steals"],
            ["BLK", "Blocks"],
            ["TO", "Turnovers"],
            ["PF", "PersonalFouls"],
            ["PTS", "FinalScore"]
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
            "FGM-A" => "", ## done by splitters
            "3PM-A" => "", ## done by splitters
            "FTM-A" => "", ## done by splitters
            "OREB" => "OffRebounds",
            "DREB" => "DefRebounds",
            "REB" => "Rebounds",
            "AST" => "Assists",
            "STL" => "Steals",
            "BLK" => "Blocks",
            "TO" => "Turnovers",
            "PF" => "PersonalFouls",
            "PTS" => "Points",
            "Min" => "Minutes"
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
        "LeagueId" => 11,
        "LeagueName" => "NASCAR",
        "FriendlyName" => "Racing",
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
        "url" =>  "http://scores.espn.go.com/nfl/scoreboard?date=" + @datestr,
        "LeagueID" => 12,
        "LeagueName" => "NFL",
        "FriendlyName" => "Football",
        "league_table" =>  "Players_NFL",
        "players_table" => "Players_NFL",
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
                "LG"
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
        "espnTeamSchema" => [
            ["1st Downs", "FirstDowns"],
            ["Total Plays", "TotalPlays"]
        ],
        "trans" => {
            "General" => {
                "1st Downs" => "FirstDowns",
                "Total Plays" => "TotalPlays",
                "Total Yards" => "TotalYards",
                "Passing" => "TotalPassingYards",
                "Rushing" => "TotalRushingYards",
                "Turnovers" => "Turnovers",
                "Penalties" => "Penalties"
            },
            "Passing" => {
                "TD" =>  "PassingTDs",
                "INT" => "PassingInterceptions",
                "RTG" => "PassingRating",
                "SACKS" => "PassingSacks",
                "YDS" => "PassingYards"
            },
            "Rushing" => {
                "CAR" => "RushingAttempts",
                "TD" => "RushingTDs",
                "LG" => "RushingLong",
                "YDS" => "RushingYards",
            },
            "Receiving" => {
                "REC" => "ReceivingCatches",
                "YDS" => "ReceivingYards",
                "LG" => "ReceivingLong",
                "TD" => "ReceivingTDs",
                "TGTS" => "ReceivingTargets",
            },
            "Defensive" => {
                "SACKS" => "DefenseSacks",
                "TD" => "DefenseTDs",
                "PD" => "DefensePassesDefended",
                "QB HTS" => "",
                "TOT" => "DefenseTackles"
            },
            "Interceptions" => {
                "INT" => "DefenseInterceptions"
            },
            "Punt Returns" => {
                "TD" => "PuntReturnTDs",
                "LG" => "PuntReturnLong",
                "YDS" => "PuntReturnYards",
                "AVG" => ""
            },
            "Kick Returns" => {
                "NO" => "KickReturns",
                "LG" => "KickReturnLong",
                "TDS" => "KickReturnTDs",
                "YDS" => "KickReturnYards"
            },
            "Kicking" => {
                "FG" => "",
                "PCT" => "",
                "LONG" => "KickingLong",
                "XP" => "",
                "PTS" => "KickingPoints"
            },
            "Punting" => {
                "NO" => "Punts",
                "YDS" => "PuntYards",
                "AVG" => "",
                "TB" => "",
                "TB/s" => "",
                "-20" => "",
                "LG" => "PuntLong"
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
                },
                "XP" => {
                    "data" => [
                        "KickingXPAttempts",
                        "KickingXPMade"
                    ],
                "delimiter" => "/"
                }
            },
            "Passing" => {
                "C/ATT" => {
                    "data" => [
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
            "PassingCompletionsPct" => {
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


    @entrypoints['NCB'] = {
        "url"  => "http://scores.espn.go.com/ncb/scoreboard?date=" + @datestr,
        "LeagueID" => 13,
        "LeagueName" => "NCB",
        "FriendlyName" => "Basketball",
        "espnSchema" => [],
        "trans" => {},
        "schema" => {}
    }

    @entrypoints['NCW'] = {
        "url" => "http://scores.espn.go.com/ncw/scoreboard?date=" + @datestr,
        "LeagueID" => 14,
        "LeagueName" => "NCW",
        "FriendlyName" => "Basketball",
        "espnSchema" => [],
        "schema" => {}
    }

    @entrypoints['WNBA'] = {
        "url" => "http://scores.espn.go.com/wnba/scoreboard?date=" + @datestr,
        "LeagueID" => 15,
        "LeagueName" => "WNBA",
        "FriendlyName" => "Basketball",
        "espnSchema" => [],
        "percents" => {},
        "splitters" => {},
        "schema" => {}
    }

    @entrypoints['NCF'] = {
        "url" => "http://scores.espn.go.com/ncf/scoreboard?date=" + @datestr,
        "LeagueID" => 16,
        "LeagueName" => "NCF",
        "FriendlyName" => "Football",
        "espnSchema" => [],
        "schema" => {}
    }

    @entrypoints['MLS'] = {
        "url" => "http://www.espnfc.us/scores?date=" + @datestr,
        "BaseURL" => "http://www.espnfc.us/",
        "PlayerBaseURL" => "http://www.espnfc.us/",
        "LeagueID" => 17,
        "LeagueName" => "MLS",
        "FriendlyName" => "Soccer",
        "scorePeriods" => [],
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
        "percents" => {},
        "schema" => {}
    }

    @entrypoints['PGA'] = {
        "url" => "http://espn.go.com/golf/leaderboard",
        "LeagueID" => 18,
        "LeagueName" => "PGA",
        "FriendlyName" => "Golf",
        "scorePeriods" => [],
        "espnSchema" => [
            "POS",
            "name",
            "TO PAR",
            "R1",
            "R2",
            "R3",
            "R4",
            "TOT",
            "THRU"
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
            "THRU" => "MissedCut",
            "TO PAR" => "ToPar"
        }
    }

      @entrypoints['MLB'] = {
        "url" => "http://espn.go.com/mlb/scoreboard?date=" + @datestr,
        "LeagueID" => 19,
        "LeagueName" => "MLB",
        "FriendlyName" => "Baseball",
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
        "espnTeamSchema" => [
            ["AB", "AtBats"],
            ["RBI", "RBI"],
            ["BB", "Walks"],
            ["SO", "Strikeouts"]
        ],
         "trans" => {
           "Pitchers" => {
                "IP" => "PitchingInnings",
                "H" => "PitchingHits",
                "R" => "PitchingRuns",
                "ER" => "PitchingEarnedRuns",
                "BB" => "PitchingWalks",
                "SO" => "PitchingStrikeouts",
                "HR" => "PitchingHomeRuns"
            },
            "Batters" => {
                "AB" => "AtBats",
                "R" => "Runs",
                "H" => "Hits",
                "RBI" => "RBI",
                "HR" => "Homeruns",
                "BB" => "Walks",
                "SO" => "Strikeouts"
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
        "url" =>  "http://scores.espn.go.com/nhl/scoreboard?date=" + @datestr,
        "LeagueID" => 20,
        "LeagueName" => "NHL",
        "FriendlyName" => "Hockey",
        "league_table" => "Players_NHL",
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
        "splitters" => {},
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
            }
        },
        "percents" => {
            "FaceoffPercent" => {
                "type" => "sum",
                "data" => [
                    "FaceoffsWon",
                    "FaceoffsLost"
                ],
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

    if league == "PGA" then
        @match_url = "http://espn.go.com/golf/leaderboard?tournamentId="
    elsif league == "NASCAR"
        @match_url = "http://espn.go.com/racing/raceresults?raceId="
    elsif league == "MLS"
        @match_url = "http://www.espnfc.us/gamecast/statistics/id/"
    else
        @match_url = "http://espn.go.com/{league}/{endpoint}?gameId="
    end

    @entrypoint = @entrypoints[@league]
    @leagueId = @entrypoint['LeagueID']

    puts "League: #{@league}"
    puts "----------------------------------------------------------------------------"
    @leagueFriendlyName  = @entrypoint['FriendlyName']
    @scorePeriods = @entrypoint['scorePeriods']

    @inheritors.each {  |parent, children|
        keys = ['espnSchema', 'schema', 'percents', 'splitters', 'trans', 'scorePeriods', 'espnTeamSchema']
        children.each { |c|
          if c ==  @league then
            keys.each {  |k|
               @entrypoint[k] = @entrypoints[parent][k]
            }
          end
        }
    }

    @scorePeriods = @entrypoint['scorePeriods']

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

#-----------------------------------------------------------------------------------------------
def get_team_info(teamLink)
    team = @client.get(teamLink)
    ## todo
end

#-----------------------------------------------------------------------------------------------
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

#-----------------------------------------------------------------------------------------------
def get_team_id(team_url)
    res = @client.get(team_url)
    parser = res.parser
    matches = parser.inner_html.match(/\?teamId=(\d+)/)

    return matches[1]
end

#-----------------------------------------------------------------------------------------------
def process_basketball_stats(mod_data)
    home_players_1 = mod_data.children[7].children
    home_players_2 = mod_data.children[9].children
    away_players_1 = mod_data.children[1].children
    away_players_2 = mod_data.children[3].children

    home_players = []
    away_players = []

    parser = @parser
    hash = @espnSchemas
    team_espn_schema = @entrypoint['espnTeamSchema']
    splitters = @splitters
    percents = @percentages
    trans = @trans

    home_players_1.each { |player|
        home_players.push({
            "element" => player,
            "teamId" => @home_team_id
        })
    }
    home_players_2.each { |player|
        if not home_players_2.xpath("//*[@colspan='14']")
            home_players.push({
                "element" => player,
                "teamId" => @home_team_id
            })
        end
    }
    away_players_1.each { |player|
        away_players.push({
            "element" => player,
            "teamId" => @away_team_id
        })
    }
    away_players_2.each { |player|
        if not home_players_2.xpath("//*[@colspan='14']")
            away_players.push({
                "element" => player,
                "teamId" => @away_team_id
            })
        end
    }

    final_players = home_players + away_players
    players = {}
    final_players.each { |player_|
        player = player_['element']
        teamId = player_['teamId']
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
      team_stats = {
      }

      # Team stats are shown on ESPN data as represented on the
      # espnTeamSchema order. Example: The first column is FGM-A, second 3PM-A, etc, and the last column is PTS
      teams = parser.xpath("//*[@class='even']")
      home_stats = teams.last.children
      away_stats = teams.first.children

      cnt = 0
      team_stats[@home_acc] = {}
      team_stats[@away_acc] = {}

      home_stats.each do |stat|
        html_data = stat.children.inner_html
        if !html_data.empty?
          espn_schema = team_espn_schema[cnt]
          data_acronym = espn_schema[0]
          database_column = espn_schema[1]

          if database_column == 'Splitter'
            # Splitting information
            made_column, taken_column = splitters[data_acronym]
            made_data, taken_data = html_data.split('-')
            team_stats[@home_acc][made_column]  = made_data
            team_stats[@home_acc][taken_column] = taken_data
          else
            team_stats[@home_acc][database_column] = html_data
          end

          cnt += 1
        end
      end

      cnt = 0

      away_stats.each do |stat|
        html_data = stat.children.inner_html
        if !html_data.empty?
          espn_schema = team_espn_schema[cnt]
          data_acronym = espn_schema[0]
          database_column = espn_schema[1]

          if database_column == 'Splitter'
            # Splitting information
            made_column, taken_column = splitters[data_acronym]
            made_data, taken_data = html_data.split('-')
            team_stats[@away_acc][made_column]  = made_data
            team_stats[@away_acc][taken_column] = taken_data
          else
            team_stats[@away_acc][database_column] = html_data
          end

          cnt += 1
        end
      end

      percents.each { |k, percent|

            if team_stats[@away_acc] then
              if team_stats[@away_acc][percent['lower']] and team_stats[@away_acc][percent['upper']] then
                lower = team_stats[@away_acc][percent['lower']].to_f
                upper = team_stats[@away_acc][percent['upper']].to_f


                if lower > 0
                  percentage = (lower / upper) * 100
                else
                  percentage = 0
                end

                team_stats[@away_acc][k] = String.new(percentage.to_s) + "%"
              end
            end

            if team_stats[@home_acc] then
              if team_stats[@home_acc][percent['lower']] and team_stats[@home_acc][percent['upper']] then
                lower = team_stats[@home_acc][percent['lower']].to_f
                upper = team_stats[@home_acc][percent['upper']].to_f


                if lower > 0
                  percentage = (lower / upper) * 100
                else
                  percentage = 0
                end

                team_stats[@home_acc][k] = String.new(percentage.to_s) + "%"
              end
            end
        }

      return {
        "players"=> players,
        "teams"=> team_stats
      }
end

#-----------------------------------------------------------------------------------------------
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

#-----------------------------------------------------------------------------------------------
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

#-----------------------------------------------------------------------------------------------
def process_struct_of_data_soccer(schema, data, trans, type)
    players = {}
    url_base = 'http://www.espnfc.us/'
    data.children.each { |player|
        if player.class.to_s == "Nokogiri::XML::Element" then
            stats = player.xpath("td")

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

            stats.each { |stat|
                if not stat.class.to_s == "Nokogiri::XML::Element" then
                    next
                end

                if cnt ==  2 then
                    cnt += 1
                    next
                end

                stat = stat.inner_html
                stat = stat.gsub(/\n|\r|\s+/, "")

                if trans[schema[cnt]] then
                    players[player_name][trans[schema[cnt]]] = stat
                end

                cnt += 1
            }
        end
    }

    return players
end

#-----------------------------------------------------------------------------------------------
def process_racing_stats(modData)
    odd_players  = modData.xpath("//tr[contains(@class, 'oddrow player')]")
    even_players = modData.xpath("//tr[contains(@class, 'everow player')]")

    players_final = {}
    players = odd_players + even_players

    schema = @espnSchemas
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

        stats.each { |stat|
            if cnt == 1 then
                cnt += 1
                next
            end

            players_final[name][@trans[schema[cnt]]] = stat.inner_html
            cnt += 1
        }
    }

    return {
        "players" => players_final,
        "teams" => {}
    }
end

#-----------------------------------------------------------------------------------------------
def process_golf_stats(modData)
    link_base = "http://espn.go.com/golf/player/_/id/"

    players = modData.xpath("//tr[contains(@id, 'player-')]")
    status = modData.xpath("//*[@class='round']")
    gamestatus = status[0].children[0].inner_text
    players_final = {}

    players.each { |player|
        stats = player.xpath("td")

        pos     = stats[0].children[0].inner_text

        if gamestatus == "Complete"
            id      = stats[2].children[0].attr("name")
            name    = stats[2].children[0].inner_text
            to_par  = stats[3].children[0].inner_text
            thru    = 0
            r1      = stats[4].children[0].inner_text
            r2      = stats[5].children[0].inner_text
            r3      = stats[6].children[0].inner_text
            r4      = stats[7].children[0].inner_text
            strokes = stats[8].children[0].inner_text
        else
            id      = stats[3].children[0].attr("name")
            name    = stats[3].children[0].inner_text
            to_par  = stats[4].children[0].inner_text
            thru    = stats[6].children[0].inner_text
            r1      = stats[7].children[0].inner_text
            r2      = stats[8].children[0].inner_text
            r3      = stats[9].children[0].inner_text
            r4      = stats[10].children[0].inner_text
            strokes = stats[11].children[0].inner_text
        end

        players_final[name] = {}

        sname = name.downcase.gsub(/\s/, "-")
        link = link_base + id + "/"  + sname

        players_final[name]['teamId'] = 181
        players_final[name]['POS']    = pos
        players_final[name]['url']    = link
        players_final[name]['id']     = id
        players_final[name]['name']   = name
        players_final[name]['TO PAR'] = to_par
        players_final[name]['THRU']   = thru
        players_final[name]['R1']     = r1
        players_final[name]['R2']     = r2
        players_final[name]['R3']     = r3
        players_final[name]['R4']     = r4
        players_final[name]['TOT']    = strokes
    }

    return {
        "players" => players_final,
        "teams" => {}
    }
end

#-----------------------------------------------------------------------------------------------
def process_baseball_stats(modData, parser)
    home_batters       = modData[0].children[1]
    home_team_batting  = modData[0].children[2]
    away_batters       = modData[2].children[1]
    away_team_batting  = modData[2].children[2]
    if @inProgress == 1
        home_pitchers      = modData[1].children[1]
        home_team_pitching = modData[1].children[2]
        away_pitchers      = modData[3].children[1]
        away_team_pitching = modData[3].children[2]
    else
        home_pitchers      = modData[1].children[2]
        home_team_pitching = modData[1].children[3]
        away_pitchers      = modData[3].children[2]
        away_team_pitching = modData[3].children[3]
    end

    pitchers_schema = @espnSchemas['Pitchers']
    batters_schema  = @espnSchemas['Batters']
    pitchers_trans  = @trans['Pitchers']
    batters_trans   = @trans['Batters']

    players = {}

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
        away_stats[k] = away_batting[k]
    }

    @csplitters = @splitters['Pitchers']
    away_pitching = self.process_struct_of_data(pitchers_schema, away_team_pitching,pitchers_trans, "team")
    away_pitching.each { |k,v|
        away_stats[k] = away_pitching[k]
    }

    # Runs, Hits and Errors
    els = parser.xpath("//*[@class='linescore']")
    teams_totals_rhe = []
    teams_rhe_html = els.children[2].xpath("//*[contains(@style, 'font-weight:bold')]")
    teams_rhe_html.each do |data|
        number_data = data.inner_html.gsub(/\s+/, "")
        if is_number(number_data)
            teams_totals_rhe.push(number_data)
        end
    end

    teams_rhe_size = teams_totals_rhe.length / 2
    home_rhe = teams_totals_rhe.slice(0, teams_rhe_size)
    away_rhe = teams_totals_rhe.slice(teams_rhe_size, teams_totals_rhe.length)

    home_stats['Runs']   = home_rhe[0]
    home_stats['Hits']   = home_rhe[1]
    home_stats['Errors'] = home_rhe[2]

    away_stats['Runs']   = away_rhe[0]
    away_stats['Hits']   = away_rhe[1]
    away_stats['Errors'] = away_rhe[2]

    # Format stats to return
    teams = {}
    teams[@home_acc] = home_stats
    teams[@away_acc] = away_stats

    return {
        "teams" => teams,
        "players" => players
    }
end

#-----------------------------------------------------------------------------------------------
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
      goalies_away_ = self.process_struct_of_data(goalies_schema, goalies_away, trans_goalies, "player")
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
    end

#-----------------------------------------------------------------------------------------------
def process_football_stats(modData, parser)
    players = {}
    teams = {}
    home_stats = {}
    away_stats = {}
    @csplitters = @splitters
    general_schema = {
                "1st Downs" => 0,
                "Total Plays" => 6,
                "Total Yards" => 7,
                "Passing" => 10,
                "Rushing" => 15,
                "Penalties" => 19,
                "Turnovers" => 20
        }

    passing_schema = @espnSchemas['Passing']
    passing_trans  = @trans['Passing']
    rushing_schema = @espnSchemas['Rushing']
    rushing_trans  = @trans['Rushing']
    receiving_schema = @espnSchemas['Receiving']
    receiving_trans  = @trans['Receiving']
    defensive_schema = @espnSchemas['Defensive']
    defensive_trans  = @trans['Defensive']
    interceptions_schema = @espnSchemas['Interceptions']
    interceptions_trans  = @trans['Interceptions']
    kickreturns_schema = @espnSchemas['Kick Returns']
    kickreturns_trans  = @trans['Kick Returns']
    puntreturns_schema = @espnSchemas['Punt Returns']
    puntreturns_trans  = @trans['Punt Returns']
    kicking_schema = @espnSchemas['Kicking']
    kicking_trans  = @trans['Kicking']
    punting_schema = @espnSchemas['Punting']
    punting_trans  = @trans['Punting']


    away_passing = modData[2].children[1]
    home_passing = modData[3].children[1]
    away_rushing = modData[4].children[1]
    home_rushing = modData[5].children[1]
    away_receiving = modData[6].children[1]
    home_receiving = modData[7].children[1]
    away_defensive = modData[8].children[1]
    home_defensive = modData[9].children[1]
    away_interceptions = modData[10].children[1]
    home_interceptions = modData[11].children[1]
    away_kickreturns = modData[12].children[1]
    home_kickreturns = modData[13].children[1]
    away_puntreturns = modData[14].children[1]
    home_puntreturns = modData[15].children[1]
    away_kicking = modData[16].children[1]
    home_kicking = modData[17].children[1]
    away_punting = modData[18].children[1]
    home_punting = modData[19].children[1]


    home_players = self.process_struct_of_data(rushing_schema, home_rushing, rushing_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(rushing_schema, away_rushing, rushing_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }

    home_players = self.process_struct_of_data(receiving_schema, home_receiving, receiving_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(receiving_schema, away_receiving, receiving_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }

    home_players = self.process_struct_of_data(defensive_schema, home_defensive, defensive_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(defensive_schema, away_defensive, defensive_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }

    home_players = self.process_struct_of_data(interceptions_schema, home_interceptions, interceptions_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(interceptions_schema, away_interceptions, interceptions_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }

    home_players = self.process_struct_of_data(kickreturns_schema, home_kickreturns, kickreturns_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(kickreturns_schema, away_kickreturns, kickreturns_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }

    home_players = self.process_struct_of_data(puntreturns_schema, home_puntreturns, puntreturns_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(puntreturns_schema, away_puntreturns, puntreturns_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }



    home_players = self.process_struct_of_data(punting_schema, home_punting, punting_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(punting_schema, away_punting, punting_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }

    general_schema.each { |k,v|
        away_stats[k] = modData[1].children[1].children[v].children[1].inner_text
        home_stats[k] = modData[1].children[1].children[v].children[2].inner_text
    }

    @csplitters = @splitters['Passing']
    home_players = self.process_struct_of_data(passing_schema, home_passing, passing_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(passing_schema, away_passing, passing_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }

        @csplitters = @splitters['Kicking']
    home_players = self.process_struct_of_data(kicking_schema, home_kicking, kicking_trans, "player")
    home_players.each { |k|
        players = self.generate_player(players, k, @home_team_id)
    }

    away_players = self.process_struct_of_data(kicking_schema, away_kicking, kicking_trans, "player")
    away_players.each { |k|
        players = self.generate_player(players, k, @away_team_id)
    }

    teams[@home_acc] = home_stats
    teams[@away_acc] = away_stats

    return {
        "players" => players,
        "teams" => teams
    }
end

#-----------------------------------------------------------------------------------------------
def process_player_info(data)
      link = nil
      id = nil
      name = nil

      if link_tag = data.search('a')
        link = link_tag.attr('href').value
        name = link_tag.text
        # Process the id
        id = link.match(/id\/(\d+)/)
      end

      info = {
        "id" => id,
        "url" => link,
        "name" => name
      }
      #puts info
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
#-----------------------------------------------------------------------------------------------
def process_struct_of_data(struct, mod, trans, for_)


      ## last row is always
      ## team so we don't do this
      ## when we are processing
      ## then pplayersj
      if for_ == "player"
        data = []

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
                  if cnt > 0
                    curdata = process_data_further(cnt-1, stat, struct, trans, curdata)
                  end

                  ## process the player
                  ## info

                  if cnt == 0
                    curdata = process_player_info(stat)
                  end

                  cnt += 1
                }

                curdata = self.process_percentages(curdata)
                data.push(curdata)

             end

         end


      else
        cnt = 0

        curdata = {}

        mod.children[0].children.each { |stat|
          if  cnt >= 1
            curdata = process_data_further(cnt - 1, stat, struct, trans, curdata)
          end
          cnt += 1
        }

        curdata = self.process_percentages(curdata)
        data = curdata
      end

      return data
    end

#-----------------------------------------------------------------------------------------------
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

#-----------------------------------------------------------------------------------------------
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

    return curdata
end

#-----------------------------------------------------------------------------------------------
def form_url(area, gameId)
    url = String.new(@match_url)
    url = url.gsub(/\{endpoint\}/, area)
    url = url.gsub(/\{league\}/, @league.downcase)

    if @leagueFriendlyName == "Soccer" then
        return url + gameId + "/statistics.html"
    end

    return url + gameId
end

#-----------------------------------------------------------------------------------------------
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

#-----------------------------------------------------------------------------------------------
    def parse_match(gameId)


       inProgress = 0
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
          if game['inProgress'] == 0 then
             print "This game ended and we stored its results.."
          return
          end
       end

       url = self.form_url("boxscore", pureGameId)
       puts "Game URL: #{url}"

       resp = @client.get(url)

       parser = @parser = resp.parser
       gametitle = parser.xpath("//title").first.inner_html.match(/([^^]+)\-/)

       if gametitle then
          gametitle = gametitle[1].gsub(/\s?\-.*/, "")
          gametitle = gametitle.gsub(" Golf Leaderboard and Results ", "")
       else
          gametitle = parser.xpath("//title").first.inner_html
       end

       attendance = 0

       if @league == "MLB" then
          matches = parser.inner_text.match(/Attendance([\d]+,?[\d]+)/)
       else
          matches = parser.inner_html.match(/Attendance\:[A-Za-z<>\\\/\s]+([\d]+,?[\d]+)/)
       end



       if matches then
            attendance = matches[1].gsub(/,/, "")
       end

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
            gameTime = time_of_match.children[0].inner_html
            puts "Game Time: #{gameTime}"

            startDate = Time.zone.parse(gameTime).strftime("%Y-%m-%d %H:%M:%S")
       else
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
          ##puts split
          #split =split.children[2].inner_html
          splitter = parser.inner_html.match(/var\s+?d\s+\=\s+new\s+Date\(([\d]+)\)/)

          unix_time = splitter[1].to_i / 1000

          startDate = Time.at(unix_time).strftime("%Y-%m-%d %H:%M:%S")

         end

       end


      status = parser.xpath("//*[@class='game-state']")

      not_started = false
      ended = false

      status.each { |st|
          if st.inner_html.include? "Final" then
             inProgress = 0
             @inProgress = 0
             ended = true
             puts "Game Status: Final"
          elsif st.inner_html.include? "ET" then
             inProgress = 2
             @inProgress = 2
             not_started = true
             puts "Game Status: Not Started"
          elsif st.inner_html.include? "Delayed" then
             inProgress = 2
             @inProgress = 2
             not_started = true
             puts "Game Status: Delayed"
          elsif st.inner_html.include? "Postponed" then
             inProgress = 2
             @inProgress = 2
             not_started = true
             puts "Game Status: Postponed"
          else
             inProgress = 1
             @inProgress = 1
             puts "Game Status: In Progress"
          end
      }

      home_final_score = 0
      away_final_score = 0
      ## once we have recap
      ## we can stop
      ## processing this match
      ## we will do one
      ## last round for quality
      if ended then
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

          home_final_score = matches[1].inner_html
          away_final_score = matches[0].inner_html
        end
      end


      unless @leagueId

        #if not  @leagueFriendlyName ==  "Golf"  then
        #  @leagueId = parser.inner_html.match(/sportId:\s?(\d)+/)
        #  @leagueId = @leagueId[1]
        #  self.check_league(@leagueId)
        #end
      end

      if not self.is_singular_league()

       if not @leagueFriendlyName == 'Soccer' then
         matchup = parser.xpath("//*[@id='matchup-" + @league.downcase +  "-" + gameId +"']")
         away = matchup.xpath("//*[@class='team away']")
         home = matchup.xpath("//*[@class='team home']")
         awayfull = matchup.xpath("//*[@class='team-color-strip']")
         homefull = matchup.xpath("//*[@class='team-color-strip']")


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

         away_name = away_info.inner_html
         home_name = home_info.inner_html
         away_team_url = away_info.attr("href")
         home_team_url = home_info.attr("href")

         if not @league == "NCF" then
             home_regex_match = home_team_url.match(/name\/(\w*)\//)
             away_regex_match = away_team_url.match(/name\/(\w*)\//)

             @home_acc = home_regex_match[1].upcase
             @away_acc = away_regex_match[1].upcase
         else
             @away_acc = ""
             @home_acc = ""
         end

         @home_team_id = "#{@leagueId}#{self.get_team_id(home_team_url)}"
         @away_team_id = "#{@leagueId}#{self.get_team_id(away_team_url)}"

         puts "Teams: #{away_name}(#{@away_acc}) vs. #{home_name}(#{@home_acc})"
         puts "----------------------------------------------------------------------------"

         if not_started then
         game = {
             "ESPNUrl" => url,
             "Attendance" => attendance,
             "LeagueID" => @leagueId,
             "HomeTeamId" => @home_team_id,
             "AwayTeamId" => @away_team_id,
             "StartDate" => startDate,
             "gameId" => gameId,
             "GameTitle" => gametitle,
             "InProgress" => inProgress
         }
         return self.insert_or_update_game(game)
         end

         if @league == 'MLB' then
             awayfull_info = awayfull.children[0]
             homefull_info = homefull.children[2]
             away_namefull = awayfull_info.inner_text
             home_namefull = homefull_info.inner_text
         elsif @league == 'NHL' || @league == 'NBA'
             awayfull_info = awayfull.children[0].children[1]
             homefull_info = homefull.children[1].children[1]
             away_namefull = awayfull_info
             home_namefull = homefull_info
         else
             awayfull_info = away_name
             homefull_info = home_name
             away_namefull = awayfull_info
             home_namefull = homefull_info
         end


         els = parser.xpath("//*[@class='linescore']")


         ## get the total scores


         scores = els.children[2].xpath("//*[@style = 'text-align:center']")
         scores_full = Array.new
         start = false

         max_inning = "9"

         if @league == "MLB"
            innings = els.children[2].xpath("//*[@style = 'text-align:center' and @class = 'period']")
            last_inning = innings.pop
            max_inning = String.new(last_inning)
         end

         scores.each do |score|
            score = String.new(score.inner_html.gsub(/\s+/, ""))
            if score ==  "T" or score == max_inning
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
         end

         team_scores = scores_full.length / 2
         away_scores = scores_full.slice(0, team_scores)
         home_scores = scores_full.slice(team_scores, scores_full.length)

         if not @league == "MLB"
            home_scores.pop
            away_scores.pop
         end
         #puts "Home Scores are: "
         #puts home_scores
         #puts "Away scores are: "
         #puts away_scores
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
          #puts scores
          home_scores = []
          away_scores = []
          home_scores = scores[1]
          away_scores = scores[2]

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


      if not self.is_singular_league()
        if @leagueFriendlyName == "Soccer" then
          mod_data = parser.xpath("//section[contains(@class, 'mod-container')]")
        else
          mod_data = parser.xpath("//table[contains(@class,'mod-data')]")
        end
      else
          mod_data = parser.xpath("//div[contains(@class,'mod-content')]")
      end


      if @leagueFriendlyName == "Basketball" then
          stats = process_basketball_stats(mod_data)
      end

      if @leagueFriendlyName == "Football" then
          stats = process_football_stats(mod_data, parser)
      end

      if @leagueFriendlyName == "Hockey" then
          stats = process_hockey_stats(mod_data)
      end
      if @leagueFriendlyName == "Baseball" then
          stats = process_baseball_stats(mod_data, parser)
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
           "name" => away_name,
           "fullname" => away_namefull,
           "prefix" => @away_acc,
           "url" => away_team_url,
           "id" => @away_team_id,
           "scores" => away_scores,
           "finalScore" => away_final_score,
           "stats" => team_stats[@away_acc]
        },
       "home" => {
          "name" => home_name,
          "fullname" => home_namefull,
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

#-----------------------------------------------------------------------------------------------
def insert_or_update_game(game)
    if game['InProgress'] == 1
        @task_logger.increment(:games_in_progress)
    end

    if not self.game_id_exists(game['gameId']) then
        return self.insert_game(game)
    else
        return self.update_game(game)
    end
end

#-----------------------------------------------------------------------------------------------
def game_id_exists(gameId)
    if gameId then
        q = @db.query("SELECT * FROM `Games` where GameId = '" + gameId + "'")
        return self.eval_count(q)
    end

    return -1
end

#-----------------------------------------------------------------------------------------------
def update_game(game)
    time = Time.new
    modifiedDate = time.strftime("%Y-%m-%d %H:%M:%S")

    updateStr = @dbsyntax.update_str(@db, "Games", "GameID", game['gameId'], {
        "InProgress" => game['InProgress'],
        "ModifiedDate" => modifiedDate
    })

    @db.query(updateStr)
    @task_logger.increment(:records_updated)

    if not game['InProgress'] == 2 then
        if not self.is_singular_league() then
            self.insert_or_update_team(game['gameId'], game['teams']['home'])
            self.insert_or_update_team(game['gameId'], game['teams']['away'])
        end

        game['players'].each { |k, player|
            self.insert_or_update_player(game['gameId'], player)
        }
    end
end

#-----------------------------------------------------------------------------------------------
def player_exists(playerId)
    if playerId then
        q = @db.query("SELECT * FROM `Players` WHERE PlayerId = '#{playerId};'")

        return eval_count(q)
    end

    return -1
end

#-----------------------------------------------------------------------------------------------
def eval_count(rows)
    if rows.num_rows > 0 then
        return true
    else
        return false
    end
end

#-----------------------------------------------------------------------------------------------
def team_exists(teamId)
    if teamId then
        q = @db.query("SELECT * FROM `Teams` WHERE TeamID = '" + teamId + "';")
        return self.eval_count(q)
    end

    return -1
end

#-----------------------------------------------------------------------------------------------
def game_player_exists(gameId, playerId)
    if playerId then
        q = @db.query("SELECT * FROM `" + self.get_game_player_table() + "` WHERE PlayerId = '" + playerId + "' AND GameId = '" + gameId + "'")

        return self.eval_count(q)
    end
    return -1
end

#-----------------------------------------------------------------------------------------------
def game_team_exists(gameId, teamId)
    if teamId then
        q = @db.query("SELECT * FROM #{self.get_game_team_table()} WHERE TeamID = #{teamId} AND GameID = #{gameId}")

        return self.eval_count(q)
    end

    return -1
end

#-----------------------------------------------------------------------------------------------
def insert_player(player)
    time = Time.new
    createdDate = time.strftime("%Y-%m-%d %H:%M:%S")
    modifiedDate = createdDate

    q = @dbsyntax.insert_str(@db, self.get_players_table(), {
        "PlayerID" => @playerId,
        "PlayerName" => player['name'],
        "ESPNURL" => player['url'],
        "LeagueID" =>  @leagueId,
        "TeamID" => player['teamId'],
        "CreatedDate" =>  createdDate,
        "ModifiedDate" => modifiedDate
    })

    @db.query(q)
    @task_logger.increment(:records_inserted)
end

#-----------------------------------------------------------------------------------------------
def is_singular_league()
    if @leagueFriendlyName == "Golf" or @leagueFriendlyName == "Racing" then
        return true
    end

    return false
end

#-----------------------------------------------------------------------------------------------
def insert_game_player(gameId, player)
    data = self.get_league_player_schema(player)

    q = @dbsyntax.insert_str(@db, self.get_game_player_table(), data)

    @task_logger.increment(:records_inserted)
    return @db.query(q)
end

#-----------------------------------------------------------------------------------------------
def update_player(player)
    time = Time.new
    modifiedDate = time.strftime("%Y-%m-%d %H:%M:%S")

    q = @dbsyntax.update_str(@db, self.get_players_table(), "PlayerID", @playerId, {
        "ModifiedDate" => modifiedDate
    })

    return @db.query(q)
end

#-----------------------------------------------------------------------------------------------
def update_game_player(gameId, player)
    data = self.get_league_player_schema(player, true)

    conditions = {
        'GameID' => gameId,
        'PlayerID' => @playerId
    }
    q = @dbsyntax.update_str_with_conditionals(@db, self.get_game_player_table(), conditions, data)

    @task_logger.increment(:records_updated)

    return @db.query(q)
end

#-----------------------------------------------------------------------------------------------
def insert_team(team)
    time = Time.new
    createdDate = time.strftime("%Y-%m-%d %H:%M:%S")
    modifiedDate = createdDate
    q = @dbsyntax.insert_str(@db, self.get_teams_table(), {
       "TeamId" => team['id'],
       "TeamPrefix" => team['prefix'],
       "TeamName" => team['name'],
       "TeamFullName" => team['fullname'],
       "ESPNUrl" => team['url'],
       "LeagueID" => @leagueId,
       "createdDate" => createdDate,
       "modifiedDate" => modifiedDate
    })

    @task_logger.increment(:records_inserted)
    return @db.query(q)
end
#-----------------------------------------------------------------------------------------------
def update_team(team)
    time = Time.new
    modifiedDate = time.strftime("%Y-%m-%d %H:%M:%S")

    q = @dbsyntax.update_str(@db, self.get_teams_table(), {
        "TeamID"  => team['id'],
        "TeamPrefix" => team['prefix'],
        "TeamName" => team['name'],
        "TeamFullName" => team['fullname'],
        "LeagueID" => @leagueId,
        "ESPNUrl" => team['url'],
        "ModifiedDate "=> modifiedDate
    })

    return @db.query(q)
end

#-----------------------------------------------------------------------------------------------
def get_game_team_table()
    return "TeamStats_" + @leagueFriendlyName
end

#-----------------------------------------------------------------------------------------------
def get_teams_table()
    return "Teams"
end

#-----------------------------------------------------------------------------------------------
def get_players_table()
    return "Players"
end

#-----------------------------------------------------------------------------------------------
def get_game_player_table()
    return "PlayerStats_" + @leagueFriendlyName
end

#-----------------------------------------------------------------------------------------------
def get_game_team_table()
    return "TeamStats_" + @leagueFriendlyName
end

#-----------------------------------------------------------------------------------------------
def get_league_player_schema(data, isUpdate=false)
    pred = {}

    @playerSchema.each { |k,v|
        if data[k] then
            pred[k] = data[k]
        end
    }

    time = Time.new
    modifiedDate = time.strftime("%Y-%m-%d %H:%M:%S")

    if not isUpdate then
        createdDate =  time.strftime("%Y-%m-%d %H:%M:%S")
        pred['CreatedDate'] =  createdDate

        if @league == "MLB" || @league == "NFL" || @league == "NCF" || @league == "NHL" then
            pred['PlayerID'] =  "#{data['id']}".gsub!(/\D/,"")
        else
            pred['PlayerID'] =  data['id']
        end
    end

    if not self.is_singular_league() then
        pred['TeamId'] = data['teamId']
    end

    pred['GameId'] = @currentGame['gameId']
    pred['LeagueId'] = @leagueId
    pred['CreatedDate'] = createdDate
    pred['ModifiedDate'] = modifiedDate

    if isUpdate
        pred.delete('GameId')
        pred.delete('TeamId')
        pred.delete('PlayerId')
        pred.delete('LeagueId')
        pred.delete('CreatedDate')
    end

    return pred
end

#-----------------------------------------------------------------------------------------------
    def get_league_team_schema(data, isUpdate=false)

      pred = {}
      lang = @scorePeriods
      cnt = 0
      time = Time.new
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


     # Stats
     stats = data['stats']
     @teamSchema.each do |field|
       ## check for existance
       ## here
       schema_key = field[0]
       if stats[schema_key]
         pred[schema_key] = stats[schema_key]
       end
     end

			## other things
			## that we will need
			## can be found
			## in main objects and current game

      date = time.strftime("%Y-%m-%d %H:%M:%S")

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

#-----------------------------------------------------------------------------------------------
def insert_game_team(gameId, team)
    data = self.get_league_team_schema(team)

    q = @dbsyntax.insert_str(@db, self.get_game_team_table(), data)

    @db.query(q)
    @task_logger.increment(:records_inserted)
end

#-----------------------------------------------------------------------------------------------
def update_game_team(gameId, team)
    data = self.get_league_team_schema(team)

    conditions = {
        'GameID' => gameId,
        'TeamID' => team['id']
    }
    q = @dbsyntax.update_str_with_conditionals(@db, self.get_game_team_table(), conditions, data)

    @db.query(q)
    @task_logger.increment(:records_updated)
end

#-----------------------------------------------------------------------------------------------
def insert_or_update_player(gameId, player)
    if @league == "MLB" || @league == "NFL" || @league == "NCF" || @league == "NHL" then
        @playerId = "#{player['id']}".gsub!(/\D/,"")
    else
        @playerId = player['id']
    end

    @playerId = "#{@leagueId}" + "#{@playerId}"

    player['id'] = "#{@leagueId}" + "#{player['id']}"

    pReturn = self.player_exists(@playerId)

    if pReturn then
        if self.game_player_exists(gameId, @playerId)
            self.update_game_player(gameId, player)
        else
            self.insert_game_player(gameId, player)
        end
    else
        if pReturn == -1 then
        ## undetermined
        else
            self.insert_player(player)
            self.insert_game_player(gameId, player)
        end
    end
end

#-----------------------------------------------------------------------------------------------
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
        else
            self.insert_team(team)
            self.insert_game_team(gameId, team)
        end
    end
end

#-----------------------------------------------------------------------------------------------
def insert_game(game)
    time = Time.new

    startDate = time.strftime("%Y-%m-%d %H:%M:%S")
    modifiedDate = startDate
    createdDate = startDate


    insertStr = @dbsyntax.insert_str(@db, "Games", {
        "GameId" => game['gameId'],
        "LeagueID" => game['LeagueID'],
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
    @task_logger.increment(:records_inserted)

    if not game['InProgress'] == 2 then
        if not self.is_singular_league() then
            self.insert_or_update_team(game['gameId'], game['teams']['home'])
            self.insert_or_update_team(game['gameId'], game['teams']['away'])
        end

        game['players'].each { |k, player|
            self.insert_or_update_player(game['gameId'], player)
        }
    end
end

#-----------------------------------------------------------------------------------------------
    def start
       url = @entrypoint['url']
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

            # MLB and NBA have a JavaScript interface
            if @entrypoint['LeagueName'] == 'MLB' || @entrypoint['LeagueName'] == 'NBA'
              game_ids_on_data_attribute = parser.xpath("//div[@id='scoreboard-page']").attribute('data-data').value
              game_ids_to_parse = game_ids_on_data_attribute.scan(/\?gameId=(\d+)/)
            else
              game_ids_to_parse = parser.inner_html.scan(/\?gameId=(\d+)/)
            end

            gameIds = game_ids_to_parse.each do |gId|
                if not  @to_traverse.include? gId[0] and is_number(gId[0]) then
                  @to_traverse.push(gId[0])
                end
            end

          end


        end

      end

      @to_traverse.each { |id|
         self.parse_match(id)
      }

    end

#-----------------------------------------------------------------------------------------------
def is_college_league()
    if @league == "NCF" or @league == "NCW" or @league == "NCB" then
        return true
    end

    return false
end

#-----------------------------------------------------------------------------------------------
def make_time(scrape_date)
    scrape_date.strftime('%Y%m%d')
end

#-----------------------------------------------------------------------------------------------
def update_match(opts)
    str = @dbsyntax.update_str(@db, @league_table,opts)
    @db.query(str)
end

#-----------------------------------------------------------------------------------------------
def add_match
    str = @dbsyntax.insert_str(@db, @league_table, opts)
    @db.query(str)
end

#-----------------------------------------------------------------------------------------------
end