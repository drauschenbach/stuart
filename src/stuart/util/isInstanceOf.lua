return function(obj, class)
  return type(obj) == 'table' and obj.isInstanceOf and obj:isInstanceOf(class)
end
