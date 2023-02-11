defimpl String.Chars, for: Tuple do
  def to_string(address) do
    inspect(address)
  end
end
