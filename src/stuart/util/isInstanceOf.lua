local isInstanceOf = function(obj, class)
  return obj.isInstanceOf ~= nil and obj:isInstanceOf(class) end
return isInstanceOf
