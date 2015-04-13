class TaskRunnerService
  attr_reader :task

  def initialize(task)
    @task = task
  end

  def stop!
    if @task.pid
      Process.kill(9, @task.pid)
      @task.update(pid: nil)
    end

    @task.stopped!
  end

  def run!
    process = Spawnling.new do
      @task.running!

      while @task.reload.running?
        # log = TaskLog.create(task: self, start_time: DateTime.now, league_name: league_name)

        begin
          @task.scraper.constantize.new(league_name).start
        rescue => exception
          Rails.logger.info exception.backtrace
        ensure
          # log.update(end_time: DateTime.now)
          sleep(@task.interval)
        end
      end
    end

    @task.update(pid: process.handle)
  end
end