require "json"
require "./utils.cr"

module Processing
  def process(i : Int64 | Float64) : Array(Bytes)
    [i.to_bytes]
  end

  def process(i : String) : Array(Bytes)
    [i.encode("utf8")]
  end

  def process(i : Array(JSON::Any)) : Array(Bytes)
    i.map { |e| process(e.raw) }.to_a
  end

  def process(i : Bool | Nil) : Array(Bytes)
    [Bytes[0]]
  end

  def process(i : Array(JSON::Any)) : Array(Bytes)
    i.map { |e| process(e.raw) }.flatten.to_a
  end

  def process(hash : Hash(String,JSON::Any)) : Array(Bytes)
    r = [] of Bytes

    hash.each do |k,v|
      key = (k + "_").encode("utf8")
      values = process_as_string(v.raw)
      values.each do |val|
        memory = IO::Memory.new
        memory.write_utf8 key
        memory.write_utf8 val
        r.push memory.to_slice
      end
    end
    r
  end

  def process_as_string(i : Array(JSON::Any)) : Array(Bytes)
    res = [] of Bytes
    i.each do |val|
      v = process_as_string(val.raw)
      v.each do |v_|
        res.push v_
      end
    end
    res
  end

  def process_as_string(i : Hash(String,JSON::Any)) : Array(Bytes)
    process i
  end

  def process_as_string(i : Int64 | Float64 | Bool | Nil | String) : Array(Bytes)
    [i.to_s.gsub("_","\\_").encode("utf8")]
  end
end
