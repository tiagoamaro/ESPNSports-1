# == Schema Information
#
# Table name: tasks
#
#  id          :integer          not null, primary key
#  name        :string(255)
#  interval    :integer          default(60)
#  pid         :integer
#  league_name :string(255)      default("NBA")
#  scraper     :string(255)      default("SportsScraper")
#  status      :integer          default(0)
#  created_at  :datetime         not null
#  updated_at  :datetime         not null
#

class Task < ActiveRecord::Base
  enum status: { stopped: 0, running: 1 }

  validates :interval, numericality: { only_integer: true, greater_than: 0 }
  validates :league_name, presence: true

  before_destroy :stop!

  def stop!
    begin
      Process.kill(9, pid)
      update(pid: nil)
    rescue => exception
      logger.info exception.backtrace
    ensure
      stopped!
    end
  end

  def run!
    process = Spawnling.new do
      running!

      while self.reload.running?
        begin
          scraper.constantize.new(league_name).start
        rescue => exception
          logger.info exception.backtrace
        ensure
          sleep(interval)
        end
      end
    end

    update(pid: process.handle)
  end
end
