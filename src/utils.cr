module Utils
  extend self

  def int_from_bytes(b : Bytes)
    buf = IO::Memory.new b
    i = buf.read_bytes Int64, IO::ByteFormat::BigEndian
    i ^ Int64::MIN
  end

  def float_from_bytes(b : Bytes)
    buf = IO::Memory.new b
    i = buf.read_bytes Int64, IO::ByteFormat::BigEndian

    if i > 0
      i = Int64::MIN &- i
    end

    buf.rewind
    buf.write_bytes (i ^ Int64::MIN), IO::ByteFormat::BigEndian
    buf.rewind
    buf.read_bytes Float64, IO::ByteFormat::BigEndian
  end

  def generate_id (time = Time.utc.to_unix_f.to_bytes)
    buf_s = Bytes.new(16)
    buf = IO::Memory.new buf_s

    buf.write time
    buf.write Random::Secure.random_bytes(n: 8)

    buf_s
  end

end

struct Int64
  def to_bytes
    res = self ^ Int64::MIN

    buf_s = Bytes.new(8)
    buf = IO::Memory.new buf_s

    buf.write_bytes res, format: IO::ByteFormat::BigEndian

    buf_s
  end

  def to_lexical
    self ^ Int64::MIN
  end
end

struct Float64
  def to_bytes
    buf_s = Bytes.new(8)
    buf = IO::Memory.new buf_s
    buf.write_bytes self

    buf.rewind
    i = buf.read_bytes(Int64)
    if i < 0
      i = Int64::MIN - i
    end

    buf.rewind
    buf.write_bytes (i ^ Int64::MIN), IO::ByteFormat::BigEndian
    buf_s
  end
end

struct UInt64
  def to_bytes
    buf_s = Bytes.new(8)
    buf = IO::Memory.new buf_s
    buf.write_bytes self
    buf_s
  end
end
