module Resque
  # Methods used by various classes in Resque.
  module Helpers
    # Direct access to the Redis instance.
    def mongo
      Resque.mongo
    end

    def mongo_workers
      Resque.mongo_workers
    end

    def mongo_stats
      Resque.mongo_stats
    end

    # Given a word with dashes, returns a camel cased version of it.
    #
    # classify('job-name') # => 'JobName'
    def classify(dashed_word)
      dashed_word.split('-').each { |part| part[0] = part[0].chr.upcase }.join
    end

    # Given a camel cased word, returns the constant it represents
    #
    # constantize('JobName') # => JobName
    def constantize(camel_cased_word)
      camel_cased_word = camel_cased_word.to_s

      if camel_cased_word.include?('-')
        camel_cased_word = classify(camel_cased_word)
      end

      names = camel_cased_word.split('::')
      names.shift if names.empty? || names.first.empty?

      constant = Object
      names.each do |name|
        constant = constant.const_get(name) || constant.const_missing(name)
      end
      constant
    end
  end
end
