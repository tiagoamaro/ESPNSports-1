class TaskRunnerService
  attr_reader :task

  def initialize(task)
    @task = task
  end

  def stop!
    begin
      Process.kill(9, @task.pid)
    rescue => exception
      Rails.logger.info exception.backtrace
    end

    @task.update(pid: nil)
    @task.stopped!
  end

  def run!
    process = Spawnling.new do
      @task.running!

      while @task.reload.running?
        @task_logger = TaskLog.create(task: @task, start_time: DateTime.now, league_name: @task.league_name)

        begin
          @task.scraper.constantize.new(league_name, @task_logger).start
        rescue => exception
          Rails.logger.info exception.backtrace
        ensure
          @task_logger.update(end_time: DateTime.now)
          sleep(@task.interval)
        end
      end
    end

    @task.update(pid: process.handle)
  end
end