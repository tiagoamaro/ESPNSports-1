require 'rails_helper'

RSpec.describe Task, type: :model do
  it { is_expected.to have_many(:logs).dependent(:destroy).class_name('TaskLog') }

  it { is_expected.to validate_numericality_of(:interval).only_integer.is_greater_than(0) }
  it { is_expected.to validate_presence_of(:league_name) }
end
