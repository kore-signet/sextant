alias Key = String
alias SelectRange = Tuple(Key, Int64 | Float64, Int64 | Float64)
alias SelectEquality = Tuple(Key, Key)
alias Selector = SelectRange | SelectEquality | Tuple(Symbol, Array(Selector))

def where (s : Key)
  SelectorBuilder.new(s)
end

def union(*selectors)
  Tuple.new(:or, selectors.to_a)
end

def intersection(*selectors)
  Tuple.new(:and, selectors.to_a)
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
end
