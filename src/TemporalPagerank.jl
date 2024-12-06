using Serialization
using Graphs
using DataFrames
using Arrow
using Plots
using StatsPlots

include("constants.jl")

function temporal_pagerank(alpha, beta, files)
    r = Dict{Int, Float64}()
    s = Dict{Int, Float64}()
    for file in files
        stream = open(joinpath(edits_dir, file*".txt"), "r")
        edge = split(readline(stream), ",")
        while (length(edge) > 1)
            if (edge[3] == "init" || edge[3] == "add")
                from = parse(Int, edge[2])
                to = parse(Int, edge[4])
                r[from] = get(r, from, 0) + (1 - alpha)
                s[from] = get(s, from, 0) + (1 - alpha)
                s[to] = get(s, to, 0) + s[from]*alpha

                s[to] = get(s, to, 0) + get(from, to, 0)*(1 - beta)*alpha
                s[from] = get(s, from, 0)*beta  
            end
            edge = split(readline(stream), ",")
        end
    end
    return r
end

f = String[]
for year in 2001:2001
    for i in 1:9
        month = string(year)*"-0"*string(i)
        push!(f, month)
    end
    for j in 10:12
        month = string(year)*"-"*string(j)
        push!(f, month)
    end
end

r = temporal_pagerank(0.85, 0.99, f)
val_key_list = [(v, k) for (k, v) in r]
sorted_list = sort(val_key_list, by = x -> x[1])
println(sorted_list[1:10])
