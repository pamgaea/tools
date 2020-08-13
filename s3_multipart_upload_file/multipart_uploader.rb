#!/usr/bin/env ruby
# frozen_string_literal: true

require 'aws-sdk-s3'

class MultipartUploader
  PART_SIZE = 1024 * 1024 * 15 # 15MB

  def initialize(filepath:, bucket:, key:, region:, upload_id: nil, upload_start: 1, upload_parts: [])
    @filepath = filepath
    @bucket = bucket
    @key = key
    @region = region

    @upload_id = upload_id
    @upload_start = upload_start
    @upload_parts = symbolize_parts(upload_parts)
  end

  def run
    file = File.open(@filepath, 'r')

    # Create a multi-part upload if necessary. Otherwise resume an existing one.
    if @upload_id.nil?
      puts "Creating a multi-part upload."
      response = create_multipart_upload
      @upload_id = response.upload_id
      puts "-- multi-part upload id: #{upload_id}"
    else
      puts "Resuming an existing multi-part upload."
      puts "-- multi-part upload id: #{@upload_id}"
      puts "-- upload start: #{@upload_start}"
      puts "-- parts: #{@upload_parts}"
    end

    puts "\n"
    puts "Scrubbing to the last uploaded part, then starting upload..."
    last_uploaded_part = nil
    read_file_parts(file) do |part_number|
      puts "Uploading part #{part_number}"
      upload_response = upload_part(part_number, file.read(PART_SIZE))

      puts "-- uploaded part #{part_number}, etag: #{upload_response.etag}"
      last_uploaded_part = part_number
      @upload_parts << { etag: upload_response.etag.gsub("\"", ""), part_number: part_number }
    end

    puts "\n"
    puts "Completing multi-part upload."
    complete_multipart_upload

    puts "\n\n"
    puts "Multi-part Upload Done!"

  ensure
    file.close

    puts "\n\n"
    puts "Parts #{@upload_parts.to_json.inspect}"
    puts "Upload ID: #{@upload_id.inspect}"
    puts "Last Upload Part: #{last_uploaded_part.inspect}."
  end

  private

  def read_file_parts(file)
    part_number = 1
    until file.eof?
      if part_number < upload_start
        puts "Part #{part_number} has been uploaded. Skipping."
        file.read(PART_SIZE)
      else
        yield part_number
      end

      part_number += 1
    end
  end

  def create_multipart_upload
    s3_client.create_multipart_upload(
      bucket: @bucket,
      key: @key,
      content_type: "application/zip",
      content_disposition: "attachment; filename=$#{File.basename(@filepath)}"
    )
  end

  def complete_multipart_upload
    s3_client.complete_multipart_upload(
      bucket: @bucket,
      key: @key,
      upload_id: @upload_id,
      multipart_upload: { parts: @upload_parts },
    )
  end

  def upload_part(part_number, body)
    s3_client.upload_part(
      bucket: @bucket,
      key: @key,
      upload_id: @upload_id,
      body: body,
      part_number: part_number,
    )
  end

  def symbolize_parts(parts)
    parts.map do |part|
      part.map { |k, v| [k.to_sym, v] }.to_h
    end
  end

  def s3_client
    @s3_client ||= Aws::S3::Client.new(region: @region)
  end
end
