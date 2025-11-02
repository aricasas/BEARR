fn reverse_pyramid_with_groups(vec: Vec<i32>, n: usize) -> Vec<Vec<Vec<(i32, usize)>>> {
    // First build forward pyramid and track mappings
    let mut forward = vec![];
    let mut current = vec.clone();

    loop {
        let chunks: Vec<Vec<i32>> = current.chunks(n).map(|chunk| chunk.to_vec()).collect();

        if chunks.len() <= 1 {
            forward.push(chunks);
            break;
        }

        forward.push(chunks.clone());
        current = chunks.iter().map(|chunk| *chunk.last().unwrap()).collect();
    }

    // Reverse and assign new group IDs
    forward.reverse();

    let mut result = vec![];
    let mut next_id = 1;

    for (level_idx, level) in forward.iter().enumerate() {
        if level_idx == forward.len() - 1 {
            // Bottom level: use original indices
            let bottom: Vec<Vec<(i32, usize)>> = level
                .iter()
                .map(|chunk| chunk.iter().enumerate().map(|(i, &v)| (v, i)).collect())
                .collect();
            result.push(bottom);
        } else {
            // Other levels: assign new IDs
            let with_ids: Vec<Vec<(i32, usize)>> = level
                .iter()
                .map(|chunk| {
                    chunk
                        .iter()
                        .map(|&v| {
                            let id = next_id;
                            next_id += 1;
                            (v, id)
                        })
                        .collect()
                })
                .collect();
            result.push(with_ids);
        }
    }

    result
}

fn main() {
    let v = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    println!("{}", reverse_pyramid_with_groups(v, 3));
}
