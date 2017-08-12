local _ = require 'lodash'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

-- algo: https://github.com/apache/spark/blob/v2.2.0/examples/src/main/scala/org/apache/spark/examples/SparkTC.scala
-- data: http://www.geeksforgeeks.org/transitive-closure-of-a-graph/
describe('Apach Spark examples', function()

  local sc = stuart.NewContext()

  function transitiveClosure(graph)
    local slices = 2
    local tc = sc:parallelize(graph, slices):cache()
    
    -- Linear transitive closure: each round grows paths by one edge,
    -- by joining the graph's edges with the already-discovered paths.
    -- e.g. join the path (y, z) from the TC with the edge (x, y) from
    -- the graph to obtain the path (x, z).

    -- Because join() joins on keys, the edges are stored in reversed order.
    local edges = tc:map(function(x) return {x[2], x[1]} end)
    
    -- This join is iterated until a fixed point is reached.
    local oldCount = 0
    local nextCount = tc:count()
    repeat
      oldCount = nextCount
      -- Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      -- then project the result to obtain the new (x, z) paths.
      local newEdges = tc:join(edges):map(function(x) return {x[2][2], x[2][1]} end)
      tc = tc:union(newEdges):distinct():cache()
      nextCount = tc:count()
    until nextCount == oldCount
    return tc
  end
  
  function toMatrix(graph, numVertices)
    -- create the matrix
    local m = {}
    for i = 1, numVertices do
      m[i] = {}
      for j = 1, numVertices do
        m[i][j] = 0
        if i == j then m[i][j] = 1 end -- vertices can reach themselves
      end
    end
    -- populate it
    for x, pair in ipairs(graph) do
      local i = pair[1]
      local j = pair[2] 
      m[i+1][j+1] = 1
    end
    return m
  end

  --     (0)------->(3)
  --      |         /|\
  --      |          |
  --      |          |
  --     \|/         |
  --     (1)------->(2)
  it('computes transitive closure on graph #1', function()
    local edges = {
      {0,1},
      {0,3},
      {1,2},
      {2,3}
    }
    local result = transitiveClosure(edges)
    local actual = toMatrix(result:collect(), 4)
    local expected = {
      {1,1,1,1},
      {0,1,1,1},
      {0,0,1,1},
      {0,0,0,1}
    }
    assert.same(expected, actual)
  end)
  
  --     (0)--->(1)
  --     /|\   /
  --      |  /
  --     \|/_
  --     (2)--->(3)
  it('computes transitive closure on graph #2', function()
    local edges = {
      {0,1},
      {0,2},
      {1,2},
      {2,0},
      {2,3},
    }
    local result = transitiveClosure(edges)
    local actual = toMatrix(result:collect(), 4)
    local expected = {
      {1,1,1,1},
      {1,1,1,1},
      {1,1,1,1},
      {0,0,0,1}
    }
    assert.same(expected, actual)
  end)
  
end)
