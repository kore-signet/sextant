require "lmdb"

abstract class Handle
  alias LimitedKeyType = String | Bytes
  abstract def put (key : LimitedKeyType, val : Bytes)
  abstract def close
  abstract def get (key : LimitedKeyType)
  abstract def get_iter (key : LimitedKeyType)
  abstract def get_iter_ge (key : LimitedKeyType)
  abstract def get_bounded (min : Int64 | Float64, max : Int64 | Float64)
  abstract def get_dups (key : LimitedKeyType)
end

class BaseHandle < Handle
  property cursor : Lmdb::Cursor
  property txn : Lmdb::Transaction

  def initialize(@cursor, @txn)
  end

  def close
    @cursor.close
    @txn.commit
  end

  def put(key : Float64 | Int64, id : Bytes)
    @cursor.put key.to_bytes, id
  end

  def put(key : String | Bytes, id : Bytes)
    @cursor.put key, id
  end

  def put(key_array : Array(String | Int64 | Float64), id : Bytes)
    key_array.each do |key|
      put key, id
    end
  end

  def get(key : Float64 | Int64)
    get key.to_bytes
  end

  def get(key : String | Bytes)
    @cursor.find key
  end

  def get_dups(key : Float64 | Int64)
    get_dups key.to_bytes
  end

  def get_dups(key : String | Bytes)
    first = (get key)[2]
    if first == nil
      return ([] of Bytes).each
    end
    [first.as(Bytes)].each.chain(DupIterator.new(@cursor))
  end

  def get_iter(key : Float64 | Int64)
    get_iter key.to_bytes
  end

  def get_iter(key : String | Bytes)
    first = (get key)[2]
    if first == nil
      return ([] of Bytes).each
    end

    [first.as(Bytes)].each.chain(ValueNextIterator.new(@cursor))
  end

  def get_iter_ge(key : Float64 | Int64)
    get_iter_ge key.to_bytes
  end

  def get_iter_ge(key : String | Bytes)
    first = @cursor.find_ge(key)
    [first[2].as(Bytes)].each.chain(ValueNextIterator.new(@cursor))
  end

  def get_bounded(key : Float64 | Int64)
    get_bounded key.to_bytes
  end

  def get_bounded(min : Float64 | Int64, max : Float64 | Int64)
  #  first = @cursor.find_ge(val)
    min = min.to_bytes
    max = max.to_bytes
    first = @cursor.find_ge(min)
    [first[2].as(Bytes)].each.chain(BoundedIterator.new(@cursor,min,max))
  end
end

class MultiHandle
  alias KeyType = String | Bytes | Int64 | Float64
  property handles : Hash(String,BaseHandle)
  property store_handles : Hash(String,BaseHandle)

  def initialize(@handles,@store_handles)
  end

  def close
    @handles.each.chain(@store_handles.each).each do |_,to_close|
      to_close.close
    end
  end

  # query methods

  def query(s : Tuple(Symbol, Array(Selector)))
    if s[0] == :or
      response_iters = [] of Iterator(Bytes)
      s[1].each do |selector|
        response_iters.push(query (selector))
      end

      response_iters.flatten
    else
      responses = [] of Array(Bytes)
      s[1].each do |selector|
        responses.push((query (selector)).to_a)
      end

      responses.reduce do |first,second|
        i = 0
        j = 0
        first_len = first.size
        second_len = second.size
        common = Array(Bytes).new 
        while i < first_len && j < second_len
          cmp = first[i] <=> second[j]
          if cmp == 0
            common.push(first[i])
            i += 1
            j += 1
          elsif cmp < 0
            i += 1
          else
            j += 1
          end
        end
        common
      end
    end
  end

  def query(s : Tuple(String, KeyType))
    @handles[s[0]].get_dups s[1]
  end

  def query(s : Tuple(String, Int64 | Float64, Int64 | Float64))
    @handles[s[0]].get_bounded s[1], s[2]
  end

  # Store methods

  def store(key : KeyType | Array(String | Int64 | Float64), val : Bytes, store_name = "store")
    @store_handles[store_name].put key, val
  end

  def fetch(key : KeyType, store_name = "store")
    @store_handles[store_name].get key
  end

  # Index Methods

  def put(idx : String, key : KeyType | Array(String | Int64 | Float64), val : Bytes)
    @handles[idx].put key, val
  end

  def get(idx : String, key : KeyType)
    @handles[idx].get key
  end

  # Index Iters

  def get_iter(idx : String, key : KeyType)
    @handles[idx].get_iter key
  end

  def get_iter_ge(idx : String, key : KeyType)
    @handles[idx].get_iter_ge key
  end

  def get_dups(idx : String, key : KeyType)
    @handles[idx].get_dups key
  end

  def get_bounded(idx : String, min : Int64 | Float64, max : Int64 | Float64)
    @handles[idx].get_bounded min, max
  end
end
