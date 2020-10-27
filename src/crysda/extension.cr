# :nodoc:
class Array(T)
  def map_non_nil(&block : T -> U) forall U
    Array(U?).new(size) do |i|
      val = @buffer[i]
      next nil if val.nil?
      yield val.not_nil!
    end
  end

  def nil_as_false
    map { |v| v.nil? ? false : v }
  end

  def nil_as_true
    map { |v| v.nil? ? true : v }
  end

  def false_as_nil
    map { |v| v ? v : nil }
  end

  def true_as_nil
    map { |v| v ? nil : v }
  end

  def not
    {% raise "method '{{@def.name}}' only applicable to Bool types" unless T <= Bool? %}
    self.map { |v| v.try &.! }
  end

  def and(other : Array(Bool?))
    {% raise "method '{{@def.name}}' only applicable to Bool types" unless T <= Bool? %}
    self.zip(other).map do |first, second|
      if first.nil? && second.nil?
        nil
      elsif (!first.nil?) && (!second.nil?)
        first && second
      else
        first.nil? ? second : first
      end
    end
  end

  def or(other : Array(Bool?))
    {% raise "method '{{@def.name}}' only applicable to Bool types" unless T <= Bool? %}
    self.zip(other).map do |first, second|
      if first.nil? && second.nil?
        nil
      elsif (!first.nil?) && (!second.nil?)
        first || second
      else
        first.nil? ? second : first
      end
    end
  end

  def concatenate(right : Array)
    raise "method '{{@def.name}}' requires both to be of same size" unless size == right.size
    self.zip(right).map { |f, s| f.to_s + " " + s.to_s }
  end

  def mean
    {% raise "method '{{@def.name}}' only applicable to Number types" unless T <= Number %}
    return nil if empty?
    val = self
    unless T.is_a?(Float64)
      val = self.map(&.to_f64)
    end
    val.sum / val.size
  end

  def median
    {% raise "method '{{@def.name}}' only applicable to Number types" unless T <= Number %}
    return nil if empty?
    val = self
    unless T.is_a?(Float64)
      val = self.map(&.to_f64)
    end
    val.sort!
    half = val.size // 2
    lower, upper = val[...half], val[half..]
    (val.size % 2 == 0) ? (lower.last + upper.first) / 2.0 : upper.first
  end

  def sd
    {% raise "method '{{@def.name}}' only applicable to Number types" unless T <= Number %}
    return nil if size <= 1
    val = self
    unless T.is_a?(Float64)
      val = self.map(&.to_f64)
    end
    Math.sqrt(val.sv)
  end

  def sv
    {% raise "method '{{@def.name}}' only applicable to Number types" unless T <= Number %}
    return nil if empty?
    val = self
    unless T.is_a?(Float64)
      val = self.map(&.to_f64)
    end
    m = val.mean
    sum = val.reduce(0) { |accum, i| accum + (i - m)**2 }
    sum/(val.size - 1).to_f
  end

  def cumsum
    {% raise "method '{{@def.name}}' only applicable to Number types" unless T <= Number %}
    skip(1).reduce([first.to_f64]) { |list, val| list + [list.last.to_f64 + val.to_f64] }
  end

  def lead(n : Int, default : T)
    self[n...] + Array(T).new(Math.min(n, size), default)
  end

  def lag(n : Int, default : T)
    Array(T).new(Math.min(n, size), default) + self[0...size - n]
  end

  def bind_rows
    Crysda::DataFrame.bind_rows(self)
  end

  def bind_cols
    Crysda.dataframe_of(self)
  end
end

# :nodoc:
module Enumerable(T)
  def select_with_index(&block : T ->)
    ary = [] of T
    each_with_index { |e, i| ary << e if yield e, i }
    ary
  end

  def scan_left(memo)
    reduce([memo]) { |list, curval| list + [yield list.last, curval] }
  end

  def reduce_until(operation : (_, T) -> _, predicate : _ -> Bool)
    skip(1).scan_left(operation.call(nil, first), &operation).skip_while(&predicate).first?
  end

  def unzip
    {% raise "method '{{@def.name}}' require type to be Tuple(K,V)" unless T <= Tuple %}
    return ([] of T) unless size > 0
    p1 = Array(typeof(self[0][0])).new(self.size)
    p2 = Array(typeof(self[0][1])).new(self.size)
    self.each do |v|
      p1 << v[0]
      p2 << v[1]
    end
    {p1, p2}
  end
end

# :nodoc:
class String
  def to_regex
    Regex.new(self)
  end

  def with(&block : Crysda::TableExpression)
    Crysda::ColumnFormula.new(self, block)
  end

  def pad_start(len : Int, char : Char = ' ')
    io = IO::Memory.new
    self.rjust(io, len, char)
    io.to_s
  end

  def pad_end(len : Int, char : Char = ' ')
    io = IO::Memory.new
    self.ljust(io, len, char)
    io.to_s
  end

  def wrap(line_size : Int)
    self.split(Regex.new("\b.{1,#{line_size - 1}}\\b\\W?")).join("\n")
  end

  def na_as_nil(na_val : String)
    (self && self == na_val) ? nil : self
  end
end
