module Query
  alias Key = String
  alias SelectRange = Tuple(Key, Int64 | Float64, Int64 | Float64)
  alias SelectEquality = Tuple(Key, Key | Int64 | Float64 | Bytes)
  alias SelectOp = Tuple(Symbol, Key, Key)
  alias SelectIntOp = Tuple(Symbol, Key, Float64 | Int64)
  alias Selector = SelectRange | SelectEquality | SelectOp | SelectIntOp | Tuple(Symbol, Array(Selector))

  def where (s : Key)
    SelectorBuilder.new(s)
  end

  def union(*selectors)
    ContainedMultiple.new(:or, selectors.to_a)
  end

  def intersection(*selectors)
    ContainedMultiple.new(:and, selectors.to_a)
  end

  class SelectorBuilder
    property key : Key
    def initialize(@key)
    end

    def in_between(min : Int64 | Float64, max : Int64 | Float64)
      ContainedRange.new(@key, min, max)
    end

    def equals(val : Int64 | Float64)
      ContainedEquality.new(@key,val.to_bytes)
    end

    def equals(val : Key | Bytes)
      ContainedEquality.new(@key, val)
    end

    def includes(val : Key)
      ContainedStringOp.new(:includes, @key, val)
    end

    def greater(val : Int64 | Float64)
      ContainedIntOp.new(:greater, @key, val)
    end

    def lesser(val : Int64 | Float64)
      ContainedIntOp.new(:lesser, @key, val)
    end

  end

  abstract class SelectorContainer
    abstract def to_tuple
  end

  class ContainedMultiple < SelectorContainer
    property kind : Symbol
    property selectors : Array(SelectorContainer)

    def initialize(@kind,@selectors)
    end

    def to_tuple
      {@kind,@selectors}
    end
  end

  class ContainedRange < SelectorContainer
    property key : Key
    property lower : Int64 | Float64
    property upper : Int64 | Float64

    def initialize(@key,@lower,@upper)
    end

    def to_tuple
      {@key, @lower, @upper}
    end
  end

  class ContainedEquality < SelectorContainer
    property key : Key
    property val : Key | Bytes

    def initialize(@key,@val)
    end

    def to_tuple
      {@key, @val}
    end
  end

  class ContainedStringOp < SelectorContainer
    property kind : Symbol
    property key : Key
    property val : Key

    def initialize(@kind,@key,@val)
    end

    def to_tuple
      {@kind, @key, @val}
    end
  end

  class ContainedIntOp < SelectorContainer
    property kind : Symbol
    property key : Key
    property val : Int64 | Float64

    def initialize(@kind,@key,@val)
    end

    def to_tuple
      {@kind, @key, @val}
    end
  end
end
