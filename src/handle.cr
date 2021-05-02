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

  def put(key : Float64 | Int64, id : Bytes | String)
    @cursor.put key.to_bytes, id
  end

  def put(key : String | Bytes, id : Bytes | String)
    @cursor.put key, id
  end

  def put(key_array : Array(String | Int64 | Float64), id : Bytes | String)
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
    if first[2] == nil
      ChainedIter.new(WrapperArrayIter.new([] of Bytes))
    else
      ChainedIter.new(
        WrapperArrayIter.new(
          [(first[2]).as(Bytes)]
        ),
        DupIterator.new(@cursor)
      )
    end
  end

  def get_iter(key : Float64 | Int64)
    get_iter key.to_bytes
  end

  def get_iter(key : String | Bytes)
    first = get key
    if first[2] == nil
      ChainedIter.new(WrapperArrayIter.new([] of Bytes))
    else
      ChainedIter.new(
        WrapperArrayIter.new(
          [(first[2]).as(Bytes)]
        ),
        ValueNextIterator.new(@cursor)
     )
   end
  end

  def get_iter_no_dups
    @cursor.first
    NoDupTupleIterator.new(@cursor)
  end

  def get_iter_ge(key : Float64 | Int64)
    get_iter_ge key.to_bytes
  end

  def get_iter_ge(key : String | Bytes)
    first = @cursor.find_ge key
    if first[2] == nil
      ChainedIter.new(WrapperArrayIter.new([] of Bytes))
    else
      ChainedIter.new(
        WrapperArrayIter.new(
          [(first[2]).as(Bytes)]
        ),
        ValueNextIterator.new(@cursor)
     )
   end
  end

  def get_iter_le(key : Float64 | Int64)
    get_iter_le key.to_bytes
  end

  def get_iter_le(key : String | Bytes)
    first = @cursor.find_ge key
    if first[2] == nil
      ChainedIter.new(WrapperArrayIter.new([] of Bytes))
    else
      ChainedIter.new(
        WrapperArrayIter.new(
          [(first[2]).as(Bytes)]
        ),
        PrevIterator.new(@cursor)
     )
   end
  end


  def get_bounded(key : Float64 | Int64)
    get_bounded key.to_bytes
  end

  def get_bounded(min : Float64 | Int64, max : Float64 | Int64)
  #  first = @cursor.find_ge(val)
    min = min.to_bytes
    max = max.to_bytes
    first = @cursor.find_ge(min)
    if first[2] == nil
      ChainedIter.new(WrapperArrayIter.new([] of Bytes))
    else
      ChainedIter.new(
        WrapperArrayIter.new([first[2].as(Bytes)]),
        BoundedIterator.new(@cursor,min,max)
      )
    end
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


  property store_handles : Hash(String,Array(BaseHandle))
  property store_databases : Hash(String,Lmdb::Database)
  property store_txn : Lmdb::Transaction

  def initialize(@handles, @idx_txn, @databases, @store_handles, @store_txn, @store_databases)
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


  def with_store_handle(idx : String, return_cur = false)
    possible_handles = @store_handles[idx]?
    if possible_handles == nil
      @store_handles[idx] = [] of BaseHandle
      possible_handles = @store_handles[idx]
    end

    if !possible_handles.as(Array(BaseHandle)).empty?
      handle = @store_handles[idx].pop
      yield handle
      if return_cur
        @store_handles[idx].push handle
      end
    else
      cur = @store_txn.open_cursor @store_databases[idx]
      handle = BaseHandle.new cur, @store_txn
      yield handle
      if return_cur
        @store_handles[idx].push handle
      end
    end
  end
  # query methods

  def query(s : ContainedMultiple)
    if s.kind == :or
      iters = s.selectors.map { |e| query(e) }
      WrapperIterator.new (
        UnionIterator.new iters
      )
    else
      iters = s.selectors.map { |e| query(e) }
      WrapperIterator.new (
        IntersectIterator.new iters
      )
    end
  end

  def fetch_query(i : BytesIter, store_name = "store")
    v = nil
    with_store_handle store_name do |cur|
      v = FetchIterator.new i, cur.cursor
    end
    v.as(FetchIterator)
  end

  def query(s : ContainedEquality)
    v = nil
    with_handle s.key do |handle|
      v = handle.get_dups s.val
    end
    v.as(ChainedIter)
  end

  def query(s : ContainedRange)
    v = nil
    with_handle s.key do |handle|
      v = handle.get_bounded s.lower, s.upper
    end
    v.as(ChainedIter)
  end

  def query(s : ContainedStringOp)
    v = nil
    if s.kind == :includes
      with_handle s.key do |handle|
        v = handle.get_subset_iter s.val.as(String)
      end
    end
    v.as(SubsetIterator)
  end

  def query(s : ContainedIntOp)
    v = nil
    if s.kind == :lesser
      with_handle s.key do |handle|
        v = handle.get_iter_le s.val.to_bytes
      end
    elsif s.kind == :greater
      with_handle s.key do |handle|
        v = handle.get_iter_ge s.val.to_bytes
      end
    end
    v.as(ChainedIter)
  end
  # Store methods

  def store(key : KeyType | Array(String | Int64 | Float64), val : Bytes | String, store_name = "store")
    with_store_handle store_name, return_cur: true do |cur|
      cur.put key, val
    end
  end

  def fetch(key : KeyType, store_name = "store")
    v = nil
    with_store_handle store_name, return_cur: true do |cur|
      v = cur.get key
    end
    if v != nil
      v.as(Tuple(Bool | Nil, Float32 | Float64 | Int32 | Int64 | Slice(UInt8) | String | UInt32 | UInt64 | Nil, Float32 | Float64 | Int32 | Int64 | Slice(UInt8) | String | UInt32 | UInt64 | Nil))[2]
    else
      v
    end
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
    with_handle idx do |cur|
      cur.get_iter key
    end
  end

  def get_iter_no_dups(idx : String)
    i = nil
    with_handle idx do |cur|
      i = cur.get_iter_no_dups
    end
    i.as(NoDupTupleIterator)
  end
  #
  # def get_iter_ge(idx : String, key : KeyType)
  #   @handles[idx].get_iter_ge key
  # end
  #
  # def get_dups(idx : String, key : KeyType)
  #   @handles[idx].get_dups key
  # end
  #
  # def get_bounded(idx : String, min : Int64 | Float64, max : Int64 | Float64)
  #   @handles[idx].get_bounded min, max
  # end
end
