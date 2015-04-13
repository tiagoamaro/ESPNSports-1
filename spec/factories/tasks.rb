FactoryGirl.define do
  factory :task do
    sequence(:name) { |n| "Task#{n}" }
    interval 60
    league_name 'NBA'
  end
end