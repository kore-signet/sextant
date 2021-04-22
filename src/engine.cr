require "./*"
require "lmdb"
require "uuid"

class Engine
  property index
  property store

  def initialize(index_path : String, store_path : String, map_size = 5_u64 * 10_u64 ** 9_u64, max_indexes = 20)
    @index = Lmdb::Environment.new(index_path, max_db_size: map_size, max_dbs: max_indexes)
    @store = Lmdb::Environment.new(store_path, max_db_size: map_size, max_dbs: 2)
  end

  def get_store_handle (name : String | Nil)
    store_dbi = @store.open_database(name, Lmdb::Flags::Database.flags(CREATE))
    store_txn = @store.open_transaction
    store_cur = store_txn.open_cursor(store_dbi)

    return BaseHandle.new(store_cur, store_txn)
  end

  def get_idx_handle(index_name : String)
    idx_dbi = @index.open_database(index_name, Lmdb::Flags::Database.flags(CREATE, DUP_SORT, DUP_FIXED))
    idx_txn = @index.open_transaction
    idx_cur = idx_txn.open_cursor(idx_dbi)
    return BaseHandle.new(idx_cur, idx_txn)
  end

  def get_idx_handle(index_name : String)
    idx_dbi = @index.open_database(index_name, Lmdb::Flags::Database.flags(CREATE, DUP_SORT, DUP_FIXED))
    idx_txn = @index.open_transaction
    idx_cur = idx_txn.open_cursor(idx_dbi)
    return BaseHandle.new(idx_cur, idx_txn)
  end

  def with_single_handle(index_name : String)
    idx_dbi = @index.open_database(index_name, Lmdb::Flags::Database.flags(CREATE, DUP_SORT, DUP_FIXED))
    idx_txn = @index.open_transaction
    idx_cur = idx_txn.open_cursor(idx_dbi)

    yield BaseHandle.new(idx_cur, idx_txn)

    idx_cur.close
    idx_txn.commit
  end

  def with_handle(index_names : Array(String), store_names = ["store","config"])
    handles = Hash(String,BaseHandle).new
    store_handles = Hash(String,BaseHandle).new

    curs = [] of Lmdb::Cursor
    dbs = Hash(String,Lmdb::Database).new
    store_dbs = Hash(String,Lmdb::Database).new

    index_names.each do |handle_name|
      dbs[handle_name] = @index.open_database handle_name, Lmdb::Flags::Database.flags(CREATE, DUP_SORT, DUP_FIXED)
    end

    store_names.each do |store_name|
      store_dbs[store_name] = @store.open_database store_name, Lmdb::Flags::Database.flags(CREATE)
    end


    store_txn = @store.open_transaction
    txn = @index.open_transaction

    index_names.each do |handle_name|
      idx_cur = txn.open_cursor(dbs[handle_name])
      curs.push idx_cur
      handles[handle_name] = BaseHandle.new(idx_cur, txn)
    end

    store_names.each do |store_name|
      store_cur = store_txn.open_cursor(store_dbs[store_name])
      curs.push store_cur
      store_handles[store_name] = BaseHandle.new(store_cur,store_txn)
    end

    yield MultiHandle.new handles, store_handles

    curs.each &.close

    txn.commit
    store_txn.commit
  end
end
