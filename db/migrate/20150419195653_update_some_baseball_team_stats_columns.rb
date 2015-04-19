class UpdateSomeBaseballTeamStatsColumns < ActiveRecord::Migration
  def up
    remove_column 'TeamStats_Baseball', 'Hits', :integer
    remove_column 'TeamStats_Baseball', 'Errors', :integer

    add_column 'TeamStats_Baseball', 'AtBats', :integer
    add_column 'TeamStats_Baseball', 'PitchingInnings', :integer
    add_column 'TeamStats_Baseball', 'PitchingRuns', :integer
    add_column 'TeamStats_Baseball', 'PitchingEarnedRuns', :integer
    add_column 'TeamStats_Baseball', 'PitchingStrikeouts', :integer
    add_column 'TeamStats_Baseball', 'PitchingHomeRuns', :integer
  end

  def down
    add_column 'TeamStats_Baseball', 'Hits', :integer
    add_column 'TeamStats_Baseball', 'Errors', :integer

    remove_column 'TeamStats_Baseball', 'AtBats', :integer
    remove_column 'TeamStats_Baseball', 'PitchingInnings', :integer
    remove_column 'TeamStats_Baseball', 'PitchingRuns', :integer
    remove_column 'TeamStats_Baseball', 'PitchingEarnedRuns', :integer
    remove_column 'TeamStats_Baseball', 'PitchingStrikeouts', :integer
    remove_column 'TeamStats_Baseball', 'PitchingHomeRuns', :integer
  end
end

# Stats returned from the MLB scraper
# {"AtBats"=>"37",
# "Runs"=>"8",
# "PitchingInnings"=>"9.0",
# "PitchingRuns"=>"5",
# "PitchingEarnedRuns"=>"5",
# "PitchingStrikeouts"=>"3",
# "PitchingHomeRuns"=>"1"}
