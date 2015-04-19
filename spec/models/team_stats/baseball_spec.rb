# == Schema Information
#
# Table name: TeamStats_Baseball
#
#  GameID             :integer          not null
#  LeagueID           :integer          not null
#  TeamID             :integer          not null
#  Inning_1           :integer          default(0)
#  Inning_2           :integer          default(0)
#  Inning_3           :integer          default(0)
#  Inning_4           :integer          default(0)
#  Inning_5           :integer          default(0)
#  Inning_6           :integer          default(0)
#  Inning_7           :integer          default(0)
#  Inning_8           :integer          default(0)
#  Inning_9           :integer          default(0)
#  Inning_10          :integer          default(0)
#  Inning_11          :integer          default(0)
#  Inning_12          :integer          default(0)
#  Inning_13          :integer          default(0)
#  Inning_14          :integer          default(0)
#  Runs               :integer          default(0)
#  CreatedDate        :datetime
#  ModifiedDate       :datetime
#  AtBats             :integer
#  PitchingInnings    :integer
#  PitchingRuns       :integer
#  PitchingEarnedRuns :integer
#  PitchingStrikeouts :integer
#  PitchingHomeRuns   :integer
#
# Indexes
#
#  GameID    (GameID)
#  LeagueID  (LeagueID)
#  TeamID    (TeamID)
#

require 'rails_helper'

RSpec.describe TeamStats::Baseball, type: :model do
end
