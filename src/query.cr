module Query
  alias Key = String
  alias SelectRange = Tuple(Key, Int64 | Float64, Int64 | Float64)
  alias SelectEquality = Tuple(Key, Key)
  alias SelectOp = Tuple(Symbol, Key, Key)
  alias Selector = SelectRange | SelectEquality | SelectOp | Tuple(Symbol, Array(Selector))

  def where (s : Key)
    SelectorBuilder.new(s)
  end

  def union(*selectors)
    {:or, selectors.to_a}
  end

  def intersection(*selectors)
    {:and, selectors.to_a}
  end


  class SelectorBuilder
    property key : Key
    def initialize(@key)
    end

    def in_between(min : Int64 | Float64, max : Int64 | Float64)
      SelectRange.new(@key, min, max)
    end

    def equals(val : Key)
      SelectEquality.new(@key, val)
    end

    def includes(val : Key)
      SelectOp.new(:includes, @key, val)
    end
  end
end
