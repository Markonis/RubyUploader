require 'date'
require 'yaml'
require 'aws-sdk'

module S3UploadStrategy
  def prepare_files
    files = config['PATHS'].select {|p| File.exist? p}
    files = files.map do |p| 
      if File.directory? p
        Dir[ File.join(p, '**', '*') ].reject { |x| File.directory? x } 
      else
        p
      end
    end
    files.flatten
  end
  
  def bucket
    @bucket ||= create_bucket
  end

  def create_bucket
    begin
      credentials = Aws::Credentials.new(
        config['AWS_ACCESS_KEY_ID'],
        config['AWS_SECRET_ACCESS_KEY'])
      
      s3 = Aws::S3::Resource.new(
        region: config['AWS_REGION'],
        credentials: credentials)

      s3.bucket(config['S3_BUCKET'])
    rescue
      puts 'Could not initialize the AWS S3 bucket'
      exit 1
    end
  end

  def upload_file(path)
    begin
      body = File.read path
      obj = bucket.object File.join(Date.today.to_s, path)
      obj.put(body: body)
      obj.etag
    rescue
      puts "Could not upload file: #{path}"
      return false
    end
    return true
  end
end

module RSyncUploadStrategy
  def parent_path(path)
    path.split('/')[0..-2].join('/')
  end

  def upload_file(path)
    parent = parent_path(path)
    command = "rsync -rave \"ssh -i #{config['PEM']}\" #{path} #{config['EC2']}:Backup#{parent}"
    system command
    $? != 1 # Exit code is 0 if the upload was successful
  end
end

class Uploader
  attr_accessor :config

  def initialize(config_file)
    unless File.exist? config_file
      puts 'Please provide a configuration file'
      exit 1
    end
    begin
      @config = YAML.load_file config_file
    rescue
      puts 'Could not parse the configuration file'
      exit 1
    end
  end

  def upload(num_threads = 1)
    errors = []
    max_tries = config['MAX_TRIES']
    files = prepare_files.map {|p| {path: p, tries: max_tries}}
    threads = []
    num_threads.times do
      threads << Thread.new do
        until files.empty? do
          file = files.shift
          puts "Uploading #{file[:path]}"
          success = upload_file file[:path]
          unless success
            if max_tries == 0 || file[:tries] > 0
              files << file
              file[:tries] -= 1 if file[:tries] > 0
            else
              errors << file[:path]
            end
          end
        end
      end
    end

    threads.each {|t| t.join}

    puts '*' * 40
    if errors.empty?
      puts 'All files uploaded successfully!'
    else
      puts 'Failed uploads:'
      errors.each {|e| puts e}
    end
    puts '*' * 40
  end

  protected

  def prepare_files
    files = config['PATHS'].select {|p| File.exist? p}
  end

  def upload_file(path)
    false
  end
end

uploader = Uploader.new('config.yml')

if ARGV[0] == 'ec2'
  uploader.extend RSyncUploadStrategy
  uploader.upload
elsif ARGV[0] == 's3'
  uploader.extend S3UploadStrategy
  uploader.upload 10
end
