using CodecBzip2
using Printf
using Arrow
using DataFrames
using Serialization
using CodecZstd
using TranscodingStreams

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
term_list = Vector{String}()
graph_state = Dict{Int, Vector{Int}}()
num_terms = 1
counter = 0

# Add a term to dict terms_to_index if it not present
function create_mapping(term::String)
    global num_terms
    if (!haskey(term_to_index, term))
        term_to_index[term] = num_terms
        num_terms += 1
        push!(term_list, term)
    end
end

function save_mapping(file::String)
    println("Saving mapping: ", file)
    basename, ext = splitext(file)
    maps_file = joinpath(map_dir, basename*".arrow")
    list_file = joinpath(map_dir, basename*"_list.arrow")
    df = DataFrame(key=collect(keys(term_to_index)), value=collect(values(term_to_index)))
    Arrow.write(maps_file, df)
    df2 = DataFrame(terms = term_list)
    Arrow.write(list_file, df2)
end

# read through dataset, two pointer method to go through edits and redirects
# save mappings and graph states for each month
# files is the months to include [2001-01, 2001-02, ....]
function save_edits(files::Vector{String}, latest="")
    # wikilinks_file is the file in wikilinks-sorted, 
    # redirects_file contains the sorted redirects for the month
    if latest != ""
        
    end
    total_read_time = 0.0
    total_write_time = 0.0
    total_save_time = 0.0
    total_comp_time = 0.0
    total_split_time = 0.0
    m = 0
    year = 1
    batch = []
    for file in files
        if m % 12 == 0
            batch = files[(year - 1)*12 + 1 : year * 12]
            println("Decompressing year $year files: ")
            @time threaded_decompress(batch)
        end
        m += 1
        println("Processing file: ", file)
        
        basename, ext = splitext(file)
        graph_state_file = joinpath(graph_state_dir, basename*".bin")

        # Using IObuffer to write in mem
        # likely not a huge timesaver
        buffer = IOBuffer()
        outfile = open(joinpath(edits_dir, basename*".txt"), "w")

        wikilinks_file = joinpath(wikilinks_path, basename)
        redirects_file = joinpath(redirects_path, basename*".sorted.bz2")

        # two pointer approach: will use the timestamps from each line to move
        # p1: wikilinks_timestamp, p2: redirects_timestamps
        # go through files in chronological order
        wikilinks_line = ""
        wikilinks_timestamp = ""
        redirects_line = ""
        redirects_timestamp = ""

        wikilinks_stream = open(wikilinks_file, "r")
        total_read_time += @elapsed begin
            wikilinks_line_t = readline(wikilinks_stream)
        end
        total_split_time += @elapsed begin
            wikilinks_line = split(wikilinks_line_t, ',')
        end
        wikilinks_timestamp = wikilinks_line[1]

        # check for no redirects for the month
        redirects_stream = nothing
        if isfile(redirects_file)
            println("found redirects")
            redirects_stream = Bzip2DecompressorStream(open(redirects_file, "r"))
            total_read_time += @elapsed begin
                redirects_line = split(strip(readline(redirects_stream)), ",")
            end
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
                        total_write_time += @elapsed begin
                            println(buffer, "$page_timestamp,$pagenum,$init,$linknum")
                        end
                        total_read_time += @elapsed begin
                            wikilinks_line_t = readline(wikilinks_stream)
                        end
                        total_split_time += @elapsed begin
                            wikilinks_line = split(wikilinks_line_t, ',')
                        end
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
                        total_read_time += @elapsed begin
                            wikilinks_line_t = readline(wikilinks_stream)
                        end
                        total_split_time += @elapsed begin
                            wikilinks_line = split(wikilinks_line_t, ',')
                        end
                        wikilinks_timestamp = wikilinks_line[1] 
                    end
                    graph_state[pagenum] = new_links
                    total_comp_time += @elapsed begin
                        additions, removals = compare_arrays(old_links, new_links)
                    end
                    for a in additions
                        total_write_time += @elapsed begin
                            println(buffer, "$page_timestamp,$pagenum,$add,$a")
                        end
                    end
                    for r in removals
                        total_write_time += @elapsed begin
                            println(buffer, "$page_timestamp,$pagenum,$remove,$r")
                        end
                    end
                end
            elseif (wikilinks_timestamp == "" || 
                (redirects_timestamp != "" && wikilinks_timestamp > redirects_timestamp))
                # process single redirect
                from = redirects_line[3]
                to = redirects_line[7]
                create_mapping(string(from))
                create_mapping(string(to))
                fromnum = term_to_index[from]
                tonum = term_to_index[to]
                total_write_time += @elapsed begin
                    println(buffer, "$redirects_timestamp,$fromnum,$redirect,$tonum")
                end
                total_read_time += @elapsed begin
                    redirects_line = split(strip(readline(redirects_stream)), ",")
                end
                redirects_timestamp = redirects_line[1]
            end

            # flush buffer if it gets too large
            if sizeof(buffer) > MAXBUFLEN
                total_write_time += @elapsed begin
                    write(outfile, String(take!(buffer)))
                end
            end
        end
        total_write_time += @elapsed begin
            write(outfile, String(take!(buffer)))
        end
        # save mappings and graph
        close(outfile)
        if m % 12 == 0
            total_save_time += @elapsed begin
                save_mapping(file)
                serialize(graph_state_file, graph_state)
            end
            println("Year: ", year)
            println("read time: ", total_read_time)
            println("write time ", total_write_time)
            println("save time ", total_save_time)
            println("comp time ", total_comp_time)
            println("split ", total_split_time)
            total_read_time = 0.0
            total_write_time = 0.0
            total_save_time = 0.0
            total_comp_time = 0.0
            total_split_time = 0.0
            year += 1
            delete_files(batch)
        end
    end
    
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