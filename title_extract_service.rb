require 'open-uri'
require 'nokogiri'

module Extractor
  module Worker
    def self.queue
      @queue
    end

    def self.queue=(queue)
      @queue = queue
    end

    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      def perform_async(*args)
        Thread.new do
          Extractor::Worker.queue.push(worker: self, args: args)
        end
      end
    end

    def perform
      raise NotImplementedError
    end
  end

  # consumer
  class WorkerExcuting
    def self.start(concurrency = 1)
      concurrency.times do |n|
        new("Worker #{n}")
      end
    end

    def initialize(name)
      thread = Thread.new do
        loop do
          payload = Extractor::Worker.queue.pop
          worker_class = payload[:worker]
          worker_class.new.perform(*payload[:args])
        end
      end

      thread.name = name
      thread.join
    end
  end
end

class TitleExtractWorker
  include Extractor::Worker

  def perform(url)
    begin
      document = Nokogiri::HTML(open(url))
      title = document.css('html > head > title').first.content
      puts "Current worker #{Thread.current.name} excute #{title}"
    rescue
      "Unable to open #{url}"
    end
  end
end

Extractor::Worker.queue = Queue.new

SITE_URLS = Array.new(20) { "http://xem.vn" }

SITE_URLS.each_with_index do |url, index|
  TitleExtractWorker.perform_async(url)
end

Extractor::WorkerExcuting.start(4)
