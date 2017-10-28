local randomizeInPlace = function(arr)
  for i = #arr-1, 1, -1 do
    local j = math.random(i+1)
    local tmp = arr[j]
    arr[j] = arr[i]
    arr[i] = tmp
  end
  return arr
end
  
return randomizeInPlace
