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

class NoDupTupleIterator
  include Iterator(Tuple(Lmdb::ValTypes,Lmdb::ValTypes))
  def initialize(@cur : Lmdb::Cursor)
  end

  def next
    v = @cur.next_no_dup
    if v[2] == nil
      stop
    else
      { v[1],v[2] }
    end
  end
end

class BytesIter
  include Iterator(Bytes)

  def initialize()
  end

  def next
    stop
  end
end

class SubsetIterator < BytesIter
  property current_iter : BytesIter
  property cur : Lmdb::Cursor
  property bytes : String

  def initialize(@cur : Lmdb::Cursor, @bytes : String)
    @current_iter = BytesIter.new
    @cur.first
  end

  def next
    v = @current_iter.next
    if v.class == Iterator::Stop
      while true
        next_keyval = @cur.next_no_dup
        if next_keyval[2] == nil
          break
        elsif next_keyval[1].as(String).includes? @bytes
          @current_iter = DupIterator.new @cur
          return next_keyval[2].as(Bytes)
        end
      end
      stop
    else
      v
    end
  end
end

class BoundedIterator < BytesIter
  def initialize(@cur : Lmdb::Cursor, @min : Bytes, @max : Bytes)
  end

  def next
    v = @cur.next
    key = v[1].to_s.to_slice
    if v[2] == nil || (key <=> @max) > 0 || (key <=> @min) < 0
      @cur.close
      stop
    else
      v[2].as(Bytes)
    end
  end
end

class ValueNextIterator < BytesIter
  def initialize(@cur : Lmdb::Cursor)
  end

  def next
    v = @cur.next
    if v[2] == nil
      @cur.close
      stop
    else
      v[2].as(Bytes)
    end
  end
end

class PrevIterator < BytesIter
  def initialize(@cur : Lmdb::Cursor)
  end

  def next
    v = @cur.prev
    if v[2] == nil
      @cur.close
      stop
    else
      v[2].as(Bytes)
    end
  end
end

class DupIterator < BytesIter
  def initialize(@cur : Lmdb::Cursor)
  end

  def next
    v = @cur.next_dup
    if v[2] == nil
      @cur.close
      stop
    else
      v[2].as(Bytes)
    end
  end
end

class ChainedIter < BytesIter
  property iters : Array(BytesIter)
  property cur : BytesIter

  def initialize(*iters)
    @iters = Array(BytesIter).new.concat(iters.to_a)
    @cur = @iters.pop
  end

  def next
    v = @cur.next
    if v.class == Iterator::Stop
      next_iter = @iters.pop?
      if next_iter != nil
        @cur = next_iter.as(BytesIter)
        @cur.next
      else
        stop
      end
    else
      v
    end
  end
end

class WrapperArrayIter < BytesIter
  property buffer : Array(Bytes)
  def initialize(@buffer)
  end

  def next
    v = @buffer.pop?
    if v != nil
      v
    else
      stop
    end
  end
end

class ByteArrayIter
  include Iterator(Array(Bytes))
  def next
  end
end

class IntersectIterator < ByteArrayIter
  property iters_amount : Int32
  property left : Hash(Bytes, Int32)
  property iters : Array(BytesIter)

  def initialize(iters)
    @iters = Array(BytesIter).new.concat(iters)
    @iters_amount = @iters.size
    @left = Hash(Bytes,Int32).new
  end

  def next
    a = Array(Bytes).new

    if @iters.empty?
      return stop
    end

    @iters.each do |list|
      val = list.next
      if val != nil && val.class != Iterator::Stop
        x = val.as(Bytes)
        c = @left[x]?
        if c != nil
          c = c.as(Int32) + 1
          if c == @iters_amount
            @left.delete x
            a.push x
          else
            @left[x] = c
          end
        else
           @left[x] = 1
        end
      else
        @iters.delete list
      end
    end

    a
  end
end

class UnionIterator < ByteArrayIter
  include Iterator(Array(Bytes))
  property iters : Array(BytesIter)

  def initialize(iters)
    @iters = Array(BytesIter).new.concat(iters)
  end

  def next
    a = Array(Bytes).new

    if @iters.empty?
      return stop
    end

    @iters.each do |list|
      val = list.next
      if val != nil && val.class != Iterator::Stop
        a.push val.as(Bytes)
      else
        @iters.delete list
      end
    end

    a
  end
end

# yes, this is just basically iter.flatten
class WrapperIterator < BytesIter
  property buffer : Array(Bytes)
  property generator : ByteArrayIter

  def initialize(@generator : ByteArrayIter)
    @buffer = [] of Bytes
  end

  def next
    if !@buffer.empty?
      @buffer.pop
    else
      while true
        val = @generator.next
        if val.class == Iterator::Stop
          break
        elsif !val.as(Array(Bytes)).empty?
          @buffer = val.as(Array(Bytes))
          return @buffer.pop
        end
      end
      stop
    end
  end
end


class FetchIterator
  include Iterator(Bytes)
  property generator : BytesIter
  property store_cur : Lmdb::Cursor

  def initialize(@generator, @store_cur)
  end

  def next
    v = @generator.next
    if v.class == Iterator::Stop
      stop
    else
      blob = @store_cur.find v.as(Bytes)
      if blob[2] == nil
        stop
      else
        blob[2].as(Bytes)
      end
    end
  end
end
