using Serialization
using Graphs
using DataFrames
using Arrow
using Plots
using StatsPlots

include("constants.jl")

function snapshot_pagerank(file)
    file_path = joinpath(graph_state_dir, file)
    data = deserialize(open(file_path, "r"))
    println(length(keys(data)))
    @assert isa(data, Dict{Int, Vector{Int}})

    basename, ext = splitext(file)
    list_path = joinpath(map_dir, basename*"_list.arrow")
    df = DataFrame(Arrow.Table(list_path))
    terms = df.terms

    # Extract all unique nodes to initialize the graph
    all_nodes = Set{Int64}()
    for (key, vals) in data
        push!(all_nodes, key)
        for v in vals
            push!(all_nodes, v)
        end
    end
    println(length(all_nodes))
    g = DiGraph(length(all_nodes))


    # Add edges based on the dictionary data
    for (node, neighbors) in data
        for neighbor in neighbors
            add_edge!(g, node, neighbor)
        end
    end

    println("Converted to graph")
    # Compute PageRank for the directed graph
    ranks = pagerank(g)
    nzranks = []
    for r in ranks
        if r > 1e-4
            push!(nzranks, r)
        end
    end
    println("Non zero: ", length(nzranks))

    StatsPlots.density(nzranks, xlabel = "PageRank Score", ylabel = "Non Zero Density", title = "Density of PageRank Scores")
    savefig("density_plot.png")
    
    println("ran pr")
    # Pair each node with its PageRank score and sort in descending order
    ranked_nodes = sort(collect(enumerate(ranks)), by = x -> x[2], rev = true)

    # Get the top 10 results
    top_10 = ranked_nodes[1:20]

    # Display the top 10 nodes with their PageRank scores
    println("Top 10 PageRank results:")
    for (node, score) in top_10
        term = terms[node]
        println("Node: $node, Term: $term, PageRank: $score")

        # Output the PageRank scores
        #println("PageRank scores: ", ranks)
    end
end

snapshot_pagerank("2003-12.bin")