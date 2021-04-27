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
    first = get key
    ChainedIter.new(
      WrapperArrayIter.new(
        [(first[2]).as(Bytes)]
      ),
      DupIterator.new(@cursor)
    )
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
    ChainedIter.new(
      WrapperArrayIter.new([first[2].as(Bytes)]),
      BoundedIterator.new(@cursor,min,max)
    )
  end

  def get_subset_iter(s : String)
    SubsetIterator.new @cursor, s
  end
end

class MultiHandle
  alias KeyType = String | Bytes | Int64 | Float64
  property idx_txn : Lmdb::Transaction
  property databases : Hash(String,Lmdb::Database)
  property handles : Hash(String,Array(BaseHandle))
  property store_handles : Hash(String,BaseHandle)

  def initialize(@handles,@store_handles,@idx_txn,@databases)
  end

  def close_cursors
    @handles.each_value do |v|
      v.each do |handle|
        handle.cursor.close
      end
    end
  end

  def close
    @handles.each.chain(@store_handles.each).each do |_,to_close|
      to_close.close
    end
  end

  def with_handle(idx : String, return_cur = false)
    possible_handles = @handles[idx]?
    if possible_handles == nil
      @handles[idx] = [] of BaseHandle
      possible_handles = @handles[idx]
    end

    if !possible_handles.as(Array(BaseHandle)).empty?
      handle = @handles[idx].pop
      yield handle
      if return_cur
        @handles[idx].push handle
      end
    else
      cur = @idx_txn.open_cursor @databases[idx]
      handle = BaseHandle.new cur, @idx_txn
      yield handle
      if return_cur
        @handles[idx].push handle
      end
    end
  end
  # query methods

  def query(s : Tuple(Symbol,Array(Selector)))
    if s[0] == :or
      iters = s[1].map { |e| query(e) }
      WrapperIterator.new (
        UnionIterator.new iters
      )
    else
      iters = s[1].map { |e| query(e) }
      WrapperIterator.new (
        IntersectIterator.new iters
      )
    end
  end

  def query(s : Tuple(String, KeyType))
    v = nil
    with_handle s[0] do |handle|
      v = handle.get_dups s[1]
    end
    v.as(ChainedIter)
  end

  def query(s : Tuple(String, Int64 | Float64, Int64 | Float64))
    v = nil
    with_handle s[0] do |handle|
      v = handle.get_bounded s[1], s[2]
    end
    v.as(ChainedIter)
  end

  def query(s : Tuple(Symbol, Key, Key))
    v = nil
    if s[0] == :includes
      with_handle s[1] do |handle|
        v = handle.get_subset_iter s[2]
      end
    end
    v.as(SubsetIterator)
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
    with_handle idx, return_cur: true do |cur|
      cur.put key, val
    end
  end

  def get(idx : String, key : KeyType)
    with_handle idx, return_cur: true do |cur|
      cur.get key
    end
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
