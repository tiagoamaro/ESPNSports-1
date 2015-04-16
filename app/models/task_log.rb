# == Schema Information
#
# Table name: task_logs
#
#  id                :integer          not null, primary key
#  task_id           :integer
#  start_time        :datetime
#  end_time          :datetime
#  records_updated   :integer          default(0)
#  records_inserted  :integer          default(0)
#  games_in_progress :integer          default(0)
#  league_name       :string(255)
#  created_at        :datetime         not null
#  updated_at        :datetime         not null
#
# Indexes
#
#  index_task_logs_on_task_id  (task_id)
#

class TaskLog < ActiveRecord::Base
  MAX_LOGS_NUMBER = 100

  attr_reader :attrs_queue

  belongs_to :task

  validates :task, presence: true

  after_initialize :initialize_attributes_queue
  after_save :keep_max_logs

  def queue(attribute_symbol)
    @attrs_queue << attribute_symbol
  end

  def process_queue
    while attribute_to_update = @attrs_queue.pop
      old_value = self.send(attribute_to_update)
      self.send("#{attribute_to_update}=", old_value + 1)
    end
    self.save
  end

  private

  def initialize_attributes_queue
    @attrs_queue = []
  end

  def keep_max_logs
    if task.logs.count > MAX_LOGS_NUMBER
      last_logs = task.logs.last(MAX_LOGS_NUMBER)
      task.logs.where.not(id: last_logs).destroy_all
    end
  end
end
