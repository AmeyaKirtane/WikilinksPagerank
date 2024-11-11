function compare_arrays(old::Vector, new::Vector)
    old_set = Set(old)
    new_set = Set(new)

    additions = [i for i in new if i ∉ old_set]
    removals = [j for j in old if j ∉ new_set]

    return additions, removals
end
