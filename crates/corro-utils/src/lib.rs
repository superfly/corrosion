use std::path::Path;

use tracing::warn;

pub async fn read_files_from_paths<P: AsRef<Path>>(
    schema_paths: &[P],
) -> eyre::Result<Vec<String>> {
    let mut contents = vec![];

    for schema_path in schema_paths.iter() {
        match tokio::fs::metadata(schema_path).await {
            Ok(meta) => {
                if meta.is_dir() {
                    match tokio::fs::read_dir(schema_path).await {
                        Ok(mut dir) => {
                            let mut entries = vec![];

                            while let Ok(Some(entry)) = dir.next_entry().await {
                                entries.push(entry);
                            }

                            let mut entries: Vec<_> = entries
                                .into_iter()
                                .filter_map(|entry| {
                                    entry.path().extension().and_then(|ext| {
                                        if ext == "sql" {
                                            Some(entry)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .collect();

                            entries.sort_by_key(|entry| entry.path());

                            for entry in entries.iter() {
                                match tokio::fs::read_to_string(entry.path()).await {
                                    Ok(s) => {
                                        contents.push(s);
                                    }
                                    Err(e) => {
                                        warn!(
                                            "could not read schema file '{}', error: {e}",
                                            entry.path().display()
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "could not read dir '{}', error: {e}",
                                schema_path.as_ref().display()
                            );
                        }
                    }
                } else if meta.is_file() {
                    match tokio::fs::read_to_string(schema_path).await {
                        Ok(s) => {
                            contents.push(s);
                            // pushed.push(schema_path.clone());
                        }
                        Err(e) => {
                            warn!(
                                "could not read schema file '{}', error: {e}",
                                schema_path.as_ref().display()
                            );
                        }
                    }
                }
            }

            Err(e) => {
                warn!(
                    "could not read schema file meta '{}', error: {e}",
                    schema_path.as_ref().display()
                );
            }
        }
    }

    Ok(contents)
}
