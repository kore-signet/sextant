require "lmdb"
require "./utils.cr"

class NextTupleIterator
  include Iterator(Tuple(Lmdb::ValTypes,Lmdb::ValTypes))
  def initialize(@cur : Lmdb::Cursor)
  end

  def next
    v = @cur.next
    if v[2] == nil
      stop
    else
      { v[1],v[2] }
    end
  end
end

class BoundedIterator
  include Iterator(Bytes)
  def initialize(@cur : Lmdb::Cursor, @min : Bytes, @max : Bytes)
  end

  def next
    v = @cur.next
    key = v[1].to_s.to_slice
    if v[2] == nil || (key <=> @max) >= 0 || (key <=> @min) <= 0
      stop
    else
      v[2].as(Bytes)
    end
  end
end

class ValueNextIterator
  include Iterator(Bytes)
  def initialize(@cur : Lmdb::Cursor)
  end

  def next
    v = @cur.next
    if v[2] == nil
      stop
    else
      v[2].as(Bytes)
    end
  end
end

class PrevIterator
  include Iterator(Bytes)
  def initialize(@cur : Lmdb::Cursor)
  end

  def next
    v = @cur.prev
    if v[2] == nil
      stop
    else
      v[2].as(Bytes)
    end
  end
end

class DupIterator
  include Iterator(Bytes)

  def initialize(@cur : Lmdb::Cursor)
  end

  def next
    v = @cur.next_dup
    if v[2] == nil
      stop
    else
      v[2].as(Bytes)
    end
  end
end
