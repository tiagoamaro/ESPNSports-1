class TaskRunnerService
  attr_accessor :task

  def initialize(task)
    @task = task
  end

  def stop!
    begin
      Process.kill(9, @task.pid)
    rescue => exception
      Rails.logger.info '-----------------------------------'
      Rails.logger.info "Process #{@task.pid} did not exist. Moving on..."
      Rails.logger.info '-----------------------------------'
    end

    @task.update(pid: nil)
    @task.stopped!
  end

  def run!
    @task.running!

    process = Spawnling.new do
      while @task.reload.running?
        @task_logger = TaskLog.create(task: @task, start_time: Time.zone.now, end_time: Time.zone.now, league_name: @task.league_name)

        begin
          if within_scrape_date_interval?
            prepare_scrape_dates.each do |date|
              @task.scraper.constantize.new(@task.league_name, @task_logger, date).start
            end
          end
        rescue => exception
          Rails.logger.info '-------------------'
          Rails.logger.info exception.message
          Rails.logger.info '-------------------'
          exception.backtrace.each { |line| Rails.logger.info line }
        ensure
          @task_logger.update(end_time: DateTime.now)
          sleep(@task.interval)
        end
      end
    end

    @task.update(pid: process.handle)
  end

  def prepare_scrape_dates
    current_date_str = ENV['SCRAPE_DATE'] || Time.zone.now.to_s
    current_date = Time.zone.parse(current_date_str)
    [current_date, current_date.yesterday]
  end

  def within_scrape_date_interval?
    configured_interval = Rails.configuration.x.scrape_times[@task.league_name]
    return true unless configured_interval

    start_time   = Time.zone.parse(configured_interval[:start_time])
    end_time     = Time.zone.parse(configured_interval[:end_time])
    current_time = Time.zone.now

    start_time <= current_time && current_time <= end_time
  end
end