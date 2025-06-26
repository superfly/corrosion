#!/bin/bash

tables=("users" "teams" "deployments" "consul_services" "consul_checks")
default_dir="/var/lib/corrosion1/state.db"
default_corrosion_dirs=("/var/lib/corrosion2/state.db" "/var/lib/corrosion3/state.db")


# Compare all other databases against the first one
against_dir="${1:-$default_dir}"
if [ $# -gt 1 ]; then
    corrosion_dir=("${@:2}")
else
    corrosion_dir=("${default_corrosion_dirs[@]}")
fi

check_sql_diff () {
    echo "Checking table $table"
    res=$(sqldiff --summary --table "$table" "$against_dir" "$dir")
    if [ $? -ne 0 ]; then
        echo "Failed to run sqldiff on $table table between $against_dir and $dir"
        return 1
    fi

    if [ -z "$res" ]; then
        echo "Table $table doesn't exist"
        return 1
    fi

    updates=$(echo "$res" | awk '{print $2}')
    inserts=$(echo "$res" | awk '{print $4}')
    deletes=$(echo "$res" | awk '{print $6}')

    if [ "$inserts" -gt 0 ] || [ "$updates" -gt 0 ] || [ "$deletes" -gt 0 ]; then
        echo "Table $table is not consistent"
        return 1
    fi

    return 0
}


diffDb () {
    exit_code=0
    for dir in "${corrosion_dir[@]}"; do
        echo "Checking $dir against $against_dir"
        for table in "${tables[@]}"; do
            check_sql_diff "$table" "$against_dir" "$dir"
            if [ $? -ne 0 ]; then
                exit_code=1
            fi
        done
    done

    return $exit_code
}

for i in {1..40}; do
    diffDb
    if [ $? -eq 0 ]; then
        echo "All tables are consistent"
        exit 0
    fi
    sleep 30
done

echo "Inconsistent data in db"
exit 1
