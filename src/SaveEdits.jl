using CodecBzip2
using Printf
using Arrow
using DataFrames
using Serialization

include("utils.jl")
include("constants.jl")
# constants for edit operations

init = "init"
add = "add"
remove = "remove"
redirect = "redirect"

# initialize dict(string:int) to store mappings, counter to map to next int
# dict(int: Array[int])
term_to_index = Dict{String, Int}()
graph_state = Dict{Int, Vector{Int}}()
num_terms = 1

# Add a term to dict terms_to_index if it not present
function create_mapping(term::String)
    global num_terms
    if (!haskey(term_to_index, term))
        term_to_index[term] = num_terms
        num_terms += 1
    end
end

function save_mapping(file::String)
    println("Saving mapping: ", file)
    basename, ext = splitext(file)
    maps_file = joinpath(map_dir, basename*".arrow")
    df = DataFrame(key=collect(keys(term_to_index)), value=collect(values(term_to_index)))
    Arrow.write(maps_file, df)
end

# read through dataset, two pointer method to go through edits and redirects
# save mappings and graph states for each month
# files is the months to include [2001-01, 2001-02, ....]
function save_edits(files::Vector{String})
    # wikilinks_file is the file in wikilinks-sorted, 
    # redirects_file contains the sorted redirects for the month
    for file in files
        println("Processing file: ", file)
        
        basename, ext = splitext(file)
        graph_state_file = joinpath(graph_state_dir, basename*".bin")
        outfile = open(joinpath(edits_dir, basename*".txt"), "w")

        wikilinks_file = joinpath(wikilinks_path, basename*".bz2")
        redirects_file = joinpath(redirects_path, basename*".sorted.bz2")

        # two pointer approach: will use the timestamps from each line to move
        # p1: wikilinks_timestamp, p2: redirects_timestamps
        # go through files in chronological order
        wikilinks_line = ""
        wikilinks_timestamp = ""
        redirects_line = ""
        redirects_timestamp = ""

        wikilinks_stream = Bzip2DecompressorStream(open(wikilinks_file, "r"))
        wikilinks_line = split(strip(readline(wikilinks_stream)), ",")
        wikilinks_timestamp = wikilinks_line[1]

        # check for no redirects for the month
        redirects_stream = nothing
        if isfile(redirects_file)
            println("found redirects")
            redirects_stream = Bzip2DecompressorStream(open(redirects_file, "r"))
            redirects_line = split(strip(readline(redirects_stream)), ",")
            redirects_timestamp = redirects_line[1]
        else
            println("did not file file ", redirects_file)
        end

        # Using string comp to compare timestamps, should change this
        while (wikilinks_timestamp != "") || (redirects_timestamp != "")
            if (redirects_timestamp == "" || 
                (wikilinks_timestamp != "" && wikilinks_timestamp <= redirects_timestamp))
                # Process chunk of wikilinks belonging to the same page
                page = wikilinks_line[3]
                page_timestamp = wikilinks_timestamp
                create_mapping(string(page))
                pagenum = term_to_index[page]

                # If page is in the graph, we add all links as init operations
                # Otherwise, use compare_list to write additions and removals
                if (!haskey(graph_state, pagenum))
                    graph_state[pagenum] = Int[]
                    while wikilinks_timestamp != "" && wikilinks_line[3] == page
                        link = wikilinks_line[10]
                        create_mapping(string(link))
                        linknum = term_to_index[link]
                        push!(graph_state[pagenum], linknum)
                        @printf(outfile, "%s,%d,%s,%d\n", page_timestamp, pagenum, init, linknum)
                        wikilinks_line = split(strip(readline(wikilinks_stream)), ",")
                        wikilinks_timestamp = wikilinks_line[1] 
                    end
                else
                    old_links = graph_state[pagenum]
                    new_links = Int[]
                    while wikilinks_timestamp != "" && wikilinks_line[3] == page
                        link = wikilinks_line[10]
                        create_mapping(string(link))
                        linknum = term_to_index[link]
                        push!(new_links, linknum)
                        wikilinks_line = split(strip(readline(wikilinks_stream)), ",")
                        wikilinks_timestamp = wikilinks_line[1] 
                    end
                    graph_state[pagenum] = new_links
                    additions, removals = compare_arrays(old_links, new_links)
                    for a in additions
                        @printf(outfile, "%s,%d,%s,%d\n", page_timestamp, pagenum, add, a)
                    end
                    for r in removals
                        @printf(outfile, "%s,%d,%s,%d\n", page_timestamp, pagenum, remove, r)
                    end
                end
            elseif (wikilinks_timestamp == "" || 
                (redirects_timestamp != "" && wikilinks_timestamp > redirects_timestamp))
                # process single redirect
                from = redirects_line[3]
                to = redirects_line[7]
                create_mapping(string(from))
                create_mapping(string(to))
                @printf(outfile, "%s,%d,%s,%d\n", redirects_timestamp, term_to_index[from], redirect, term_to_index[to])
                redirects_line = split(strip(readline(redirects_stream)), ",")
                redirects_timestamp = redirects_line[1]
            end 
        end
        # save mappings and graph
        close(outfile)
        save_mapping(file)
        serialize(graph_state_file, graph_state)
         
    end
    println("done")
end

f = String[]
for year in 2001:2003
    for i in 1:9
        month = string(year)*"-0"*string(i)
        push!(f, month)
    end
    for j in 10:12
        month = string(year)*"-"*string(j)
        push!(f, month)
    end
end
@time save_edits(f)
println(num_terms)