import os
import sys
import json
import subprocess

def calculate_need(data):
    need = 0
    if 'need' in data:
        for uuid, ranges in data['need'].items():
            print(uuid)
            for range in ranges:
                need += range['end'] - range['start'] + 1

    if "partial_need" in data:
        for actor_id, versions in data["partial_need"].items():
            need += len(versions.keys())

    return need

def compare_heads(data1, data2):
    if "heads" in data1 and "heads" in data2:
        for uuid, head in data1["heads"].items():
                if uuid not in data2["heads"] or head != data2["heads"][uuid]:
                    return False
        return True

    return False

def main():
    config_files = sys.argv[1:]
    print(config_files)
    if len(config_files) == 0:
        print("no config file provided")
        exit(1)
    sync_states = {}

    got_need = False
    for config in config_files:
        print(f"getting sync state for {config}")
        result = subprocess.run(f"corrosion --config {config} sync generate", shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"failed to generate sync data for {config}: {result.stderr}")
            exit(1)
        print(result.stdout)
        data = json.loads('\n'.join(result.stdout.strip().split("\n")[1:]))
        actor_id = data["actor_id"]
        sync_states[actor_id] = data
        need_count = calculate_need(data)
        if need_count > 0:
            print(f"node (with config {config}) missing {need_count} versions")
            got_need = True

    if got_need:
        print("Some nodes are missing versions, check output")
        exit(1)

    if len(sync_states) <= 1:
        print("less than two nodes, skipping heads check")
        exit(0)

    actors = list(sync_states.keys())
    first_actor = actors[0]
    for actor in actors[1:]:
        if not compare_heads(sync_states[first_actor], sync_states[actor]):
            print(f"heads do not match for actors {first_actor} and {actor}")
            exit(1)
    
    print("all heads match")

if __name__ == "__main__":
    main()
