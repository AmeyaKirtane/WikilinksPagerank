using CodecBzip2
using Base.Threads

include("constants.jl")

function compare_arrays(old::Vector, new::Vector)
    old_set = Set(old)
    new_set = Set(new)

    additions = [i for i in new if i ∉ old_set]
    removals = [j for j in old if j ∉ new_set]

    return additions, removals
end

function custom_split(line::String)
    # Define the indices of the fields you want to extract
    timestamp = ""
    page = ""
    link = ""

    start = 1
    field_index = 1
    current_field = 1
    prev_idx = 0
    
    # Loop through the commas and extract only the required fields
    while current_field <= length(line)
        # Find the next comma
        idx = findnext(',', line, start)
        
        # If there's no comma, set idx to the end of the line
        if idx === nothing
            idx = length(line) + 1
        end
        
        # Extract the field between start and idx-1
        field = last(first(line, idx), idx - start + 1)

        # Check if we need this field
        if current_field == 1
            timestamp = field
        end
        if current_field == 3
            page = field
        end
        if current_field == 10
            link = field
            return timestamp, page, link
        end

        # Move start position to the character after the comma
        start = idx + 1
        current_field += 1
    end

    return "", "", ""
end

function decompress_bz2(file)
    file_path = joinpath(wikilinks_path, file)*".bz2"
    println("decompressing file: ", file_path)
    # Define output filename by replacing `.bz2` with `.txt`
    command = "ls $wikilinks_path"
    println(command)
    run(`$command`)
end

function threaded_decompress(files)
    # Ensure the number of threads is used effectively
    @threads for file in files
        decompress_bz2(file)
    end
end

function delete_files(file_list::Vector{String})
    for file in file_list
        file = joinpath(wikilinks_path, file)
        try
            if isfile(file)
                rm(file)  # Delete the file
                println("Deleted: $file")
            else
                println("File not found or not a file: $file")
            end
        catch e
            println("Error deleting $file: $e")
        end
    end
end

decompress_bz2("aaa")