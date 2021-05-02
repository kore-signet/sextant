require "../process.cr"
require "../sextant.cr"
require "json"
require "cadmium_tokenizer"
require "clim"
require "colorize"

include Processing
include Sextant

def y_or_n
  r = (gets).to_s.downcase
  if r == "yes" || r == "y"
    :yes
  elsif r == "no" || r == "n"
    :no
  else
    :unrecognized
  end
end

module SextantHelper
  class Cli < Clim
    CL    = STDOUT.tty? ? "\u001b[0G" : "\u000d \u000d"
    main do
      desc "Sextant CLI helper."
      usage "sextant [sub_command] [arguments]"

      run do |opts, args|
        puts opts.help_string
      end

      sub "index" do
        desc "Index and store one or more documents."
        usage "sextant index <INDEX_DB> <STORE_DB> <FILE>"

        option "-t DOC_TYPE", "--doctype=DOC_TYPE", type: String, desc: "How to parse the provided JSON Blob. Can be 'ndjson', 'list' or 'single'.", required: true
        argument "index_db", type: String, desc: "Path to index database", required: true
        argument "store_db", type: String, desc: "Path to store database", required: true
        argument "file", type: String, desc: "Path to the JSON blob to index", required: true

        run do |opts,args|
          print "-> ".colorize(:green), "opening databases", "\n"
          e = Engine.new args.index_db, args.store_db
          puts "..done!".colorize(:green).bold

          File.open args.file do |file|
            print "-> ".colorize(:green), "analysing json", "\n"
            if opts.doctype == "ndjson"
              l = JSON.parse(file.read_line)
              obj = l.as_h
              fields = obj.each_key.to_a
              print "Detected fields: ".colorize(:green), fields.join(", "), "\n"
              puts "Is this right? (y/n)"
              print "> "
              case y_or_n
              when :no
                puts "# TODO: FIX THIS"
                exit
              when :unrecognized
                exit
              end

              fields.each do |f|
                e.fields[f] = FieldTypes::Tag
              end

              puts "Should any of these fields be tokenized? (enter a comma-separated list, leave blank for none)"
              print "> "

              to_tokenize = gets
              tokenize_fields = to_tokenize.to_s.split(",", remove_empty: true)
              tokenize_fields.each do |f|
                e.fields["#{f}.whole"] = FieldTypes::WholeString
                e.fields[f] = FieldTypes::TokenizedString
              end

              tokenizer = Cadmium::Tokenizer::Pragmatic.new
              puts "Are any of these fields date fields?"
              print "> "

              date_fields = gets
              date_fields.to_s.split(",", remove_empty: true).each do |f|
                e.fields[f] = FieldTypes::Date
              end

              puts "Are any of these fields uuids or lists of uuids?"
              print "> "

              uuid_fields = gets
              uuid_fields.to_s.split(",", remove_empty: true).each do |f|
                e.fields[f] = FieldTypes::UUID
              end

              puts "Are any of these fields ints?"
              print "> "

              int_fields = gets
              int_fields.to_s.split(",", remove_empty: true).each do |f|
                e.fields[f] = FieldTypes::NumberInteger
              end

              puts "Are any of these fields floats?"
              print "> "

              float_fields = gets
              float_fields.to_s.split(",", remove_empty: true).each do |f|
                e.fields[f] = FieldTypes::NumberFloat
              end

              e.store_config

              print "-> ".colorize(:green), "indexing documents", "\n"
              e.with_handle e.fields.each_key.to_a, read_only: false do |cur|
                file.rewind
                i = 0
                i_s = "0"
                print i_s

                file.each_line do |blob|
                  print CL * i_s.to_s.size
                  print i_s

                  doc = Hash(String,JSON::Any).new
                  uuid = Bytes.empty

                  doc = JSON.parse(blob).as_h # convert json blob to Hashmap
                  doc.delete "id"

                  id = Utils.generate_id

                  cur.store id, blob.encode("utf8")

                  doc.each do |k,v| # for each key-val pair in doc, process val, indexing it into index [k]
                    field_type = e.fields[k]?
                    case field_type
                    when FieldTypes::TokenizedString
                      s = v.as_s # this field should be tokenized! so let's make it a string, check it's not empty, tokenize it, and insert each value.
                      if !s.empty?
                        cur.put k + ".whole", s.downcase, id # store a copy of the string for fuzzy string searches
                        tokenizer.tokenize(s).each do |to_insert|
                          cur.put k, to_insert, id
                        end
                      end
                    when FieldTypes::Date
                      to_insert = (Time::Format::ISO_8601_DATE_TIME.parse v.as_s).to_unix.to_bytes
                      cur.put k, to_insert, id
                    when FieldTypes::UUID
                      if v.as_s?
                        cur.put k, (UUID.new v.as_s).bytes.to_slice, id
                      elsif v.as_a?
                        v.as_a.each do |field|
                          cur.put k, (UUID.new field.as_s).bytes.to_slice, id
                        end
                      end
                    when FieldTypes::NumberFloat
                      cur.put k, v.as_f.to_bytes, id
                    when FieldTypes::NumberInteger
                      cur.put k, v.as_i64.to_bytes, id
                    when FieldTypes::Tag
                      process(v.raw).each do |to_insert|
                        if !to_insert.empty?
                          cur.put k, to_insert, id
                        end
                      end
                    end
                  end

                  i += 1
                  i_s = (i.to_s + " documents done!").colorize(:light_cyan)
                end
              end
            end
          end
        end
      end
    end
  end
end

SextantHelper::Cli.start(ARGV)
