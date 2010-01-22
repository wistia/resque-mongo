module Resque
  module Failure
    # A Failure backend that stores exceptions in Mongo. Very simple but
    # works out of the box, along with support in the Resque web app.
    class Mongo < Base
      def save
        data = {
          :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S"),
          :payload   => payload,
          :error     => exception.to_s,
          :backtrace => exception.backtrace,
          :worker    => worker.to_s,
          :queue     => queue
        }
        Resque.mongo_failures << data
      end

      def self.count
        Resque.mongo_failures.count
      end

      def self.all(start = 0, count = 1)
        Resque.mongo_failures.find().sort([:natural, :desc]).skip(start).limit(count).to_a
      end
      
      def self.clear
        Resque.mongo_failures.remove
      end
      
    end
  end
end
