# paths to data and output files
data_path = "/u/spectral_s2/dgleich/wikipedia/20230301/wikidump/output"
wikilinks_path = joinpath(data_path, "wikilinks-sorted")
redirects_path = joinpath(data_path, "redirects-by-month")

output_path = "/u/spectral_s2/dgleich/wikipedia/20230301/wikidump/pagerank/output"
map_dir = joinpath(output_path, "mappings")
graph_state_dir = joinpath(output_path, "graph_states")
edits_dir = joinpath(output_path, "edits")

# constants for edit operations
init = "init"
add = "add"
remove = "remove"
redirect = "redirect"