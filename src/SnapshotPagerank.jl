using Serialization
using Graphs

include("constants.jl")

function snapshot_pagerank(file)
    file_path = joinpath(graph_state_dir, file)
    data = deserialize(open(file_path, "r"))
    println(length(keys(data)))
    @assert isa(data, Dict{Int, Vector{Int}})

    # Extract all unique nodes to initialize the graph
    all_nodes = union(keys(data), reduce(union, values(data)))
    println(length(all_nodes))
    g = DiGraph(length(all_nodes))


    # Add edges based on the dictionary data
    for (node, neighbors) in data
        for neighbor in neighbors
            add_edge!(g, node, neighbor)
        end
    end

    # Compute PageRank for the directed graph
    ranks = pagerank(g)

    println("ran pr")
    # Pair each node with its PageRank score and sort in descending order
    ranked_nodes = sort(collect(enumerate(ranks)), by = x -> x[2], rev = true)

    # Get the top 10 results
    top_10 = ranked_nodes[1:10]

    # Display the top 10 nodes with their PageRank scores
    println("Top 10 PageRank results:")
    for (node, score) in top_10
        println("Node: $node, PageRank: $score")

        # Output the PageRank scores
        println("PageRank scores: ", ranks)
    end
end

snapshot_pagerank("2001-01.bin")