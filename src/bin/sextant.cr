require "../process.cr"
require "../sextant.cr"
require "json"
require "cadmium_tokenizer"
require "clim"
require "colorize"

include Processing

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
          e = Sextant::Engine.new args.index_db, args.store_db
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
              y_n = (gets).to_s.downcase

              if y_n == "y" || y_n == "yes"

              elsif y_n == "n" || y_n == "no"
                puts "oops"
                exit
              else
                puts "unrecognized option :/"
                exit
              end

              puts "Should any of these fields be tokenized? (enter a comma-separated list, leave blank for none)"
              print "> "
              to_tokenize = gets
              tokenize_fields = to_tokenize.to_s.split(",", remove_empty: true)
              tokenizer = Cadmium::Tokenizer::Pragmatic.new

              print "-> ".colorize(:green), "indexing documents", "\n"

              e.with_handle fields, read_only: false do |cur|
                file.rewind
                i = 0
                i_s = "0"
                print i_s

                file.each_line do |blob|
                  print CL * i_s.to_s.size
                  print i_s
                  doc = Hash(String,JSON::Any).new
                  uuid = Bytes.empty
                  begin
                    doc = JSON.parse(blob).as_h # convert json blob to Hashmap
                    id = UUID.new((doc.delete "id").to_s).bytes.to_slice # convert 'id' field to UUID bytes
                  rescue ex
                    puts ex.message
                    next
                  end


                  cur.store id, blob.encode("utf8")

                  doc.each do |k,v| # for each key-val pair in doc, process val, indexing it into index [k]
                    if tokenize_fields.index(k) != nil
                      s = v.as_s # this field should be tokenized! so let's make it a string, check it's not empty, tokenize it, and insert each value.
                      if !s.empty?
                        cur.put k, s, id
                        tokenizer.tokenize(s).each do |to_insert|
                          cur.put k, to_insert, id
                        end
                      end
                    elsif fields.index(k) != nil
                      process(v.raw).each do |to_insert|
                        if !to_insert.empty?
                          begin
                            cur.put k, to_insert, id
                          rescue ex
                            puts ex.message
                            puts k
                            puts to_insert
                          end
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
