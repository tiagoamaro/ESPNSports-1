require 'rails_helper'

describe TaskRunnerService, type: :service do
  let(:task_double) { double('Task') }
  let(:service) { TaskRunnerService.new(task_double) }

  describe '#prepare_scrape_dates' do
    context 'given the SCRAPE_DATE environment variable' do
      context 'SCRAPE_DATE is nil' do
        it 'returns an array with today and yesterday dates' do
          Timecop.freeze('29/04/2015') do
            today_date = Time.zone.now

            result = service.prepare_scrape_dates
            expect(result).to eq([today_date, today_date.yesterday])
          end
        end
      end

      context 'SCRAPE_DATE is filled in' do
        it 'returns an array with the date and day before the given SCRAPE_DATE' do
          ENV['SCRAPE_DATE'] = '10/04/2015'
          given_date = Time.zone.parse('10/04/2015')

          result = service.prepare_scrape_dates
          expect(result).to eq([given_date, given_date.yesterday])
        end
      end
    end
  end

  describe '#within_scrape_date_interval?' do
    context 'if custom Rails configuration of scrape_times is nil' do
      it 'returns true' do
        allow(task_double).to receive(:league_name).and_return('Something')

        result = service.within_scrape_date_interval?
        expect(result).to be_truthy
      end
    end

    context 'given a custom Rails configuration of scrape_times' do
      before(:each) do
        Rails.configuration.x.scrape_times = {
          'NBA' => { start_time: '10 AM', end_time: '4 PM' }
        }

        allow(task_double).to receive(:league_name).and_return('NBA')
      end

      it 'returns false if current time is before the start time' do
        Timecop.freeze('9 AM') do
          result = service.within_scrape_date_interval?
          expect(result).to be_falsy
        end
      end

      it 'returns false if current time is after the end time' do
        Timecop.freeze('11:30 PM') do
          result = service.within_scrape_date_interval?
          expect(result).to be_falsy
        end
      end

      it 'returns true if current time is within start and end time' do
        Timecop.freeze('3 PM') do
          result = service.within_scrape_date_interval?
          expect(result).to be_truthy
        end
      end
    end
  end
end