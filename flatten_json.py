import json
import csv
import xlsxwriter
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from collections.abc import Iterable
from .utils import validate_schema, write_csv, write_xlsx

logging.basicConfig(level=logging.DEBUG)

class FlattenJSONError(Exception):
    pass

def flatten_json(data, key_separator=".", include_long_form=True, array_handling="index", handle_collision="suffix", custom_handlers=None, config_file=None, error_handler=None, max_depth=None, sort_keys=False, key_filter=None, value_transform=None, parallel=False, output_format=None, delimiter=",", schema=None, array_transform=None):
    if config_file:
        try:
            with open(config_file, 'r') as file:
                config = json.load(file)
            key_separator = config.get('key_separator', key_separator)
            include_long_form = config.get('include_long_form', include_long_form)
            array_handling = config.get('array_handling', array_handling)
            handle_collision = config.get('handle_collision', handle_collision)
            max_depth = config.get('max_depth', max_depth)
            sort_keys = config.get('sort_keys', sort_keys)
            key_filter = config.get('key_filter', key_filter)
            value_transform = config.get('value_transform', value_transform)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            if error_handler:
                error_handler(e)
            else:
                raise FlattenJSONError(f"Error reading config file: {e}")

    if not isinstance(data, (dict, list, tuple, set)):
        raise FlattenJSONError("Input data must be a dictionary or an iterable (list, tuple, set)")

    flattened = {}
    stack = [(data, "", 0)]
    collision_counter = {}
    visited = set()

    def is_cyclic(obj):
        obj_id = id(obj)
        if obj_id in visited:
            return True
        visited.add(obj_id)
        return False

    def worker(data_chunk):
        local_flattened = {}
        local_stack = [(data_chunk, "", 0)]
        local_visited = set()

        while local_stack:
            current, prefix, depth = local_stack.pop()

            if max_depth is not None and depth >= max_depth:
                local_flattened[prefix] = current
                continue

            if isinstance(current, dict):
                if is_cyclic(current):
                    if error_handler:
                        error_handler(FlattenJSONError("Cyclic reference detected"))
                    else:
                        raise FlattenJSONError("Cyclic reference detected")
                for key, value in current.items():
                    new_key = f"{prefix}{key_separator}{key}" if prefix else key
                    
                    if new_key in local_flattened:
                        if handle_collision == "suffix":
                            while new_key in local_flattened:
                                new_key += "_duplicate"
                        elif handle_collision == "counter":
                            count = collision_counter.get(new_key, 1)
                            while f"{new_key}_{count}" in local_flattened:
                                count += 1
                            new_key = f"{new_key}_{count}"
                            collision_counter[new_key] = count
                        elif handle_collision == "error":
                            if error_handler:
                                error_handler(KeyError(f"Key collision detected for key: {new_key}"))
                            else:
                                raise KeyError(f"Key collision detected for key: {new_key}")

                    local_stack.append((value, new_key, depth + 1))

            elif isinstance(current, Iterable) and not isinstance(current, str):
                if array_handling == "index":
                    for i, item in enumerate(current):
                        local_stack.append((item, f"{prefix}{key_separator}{i}", depth + 1))
                elif array_handling == "concatenate":
                    local_flattened[prefix] = ", ".join(map(str, current))
                elif array_handling == "ignore":
                    continue
            else:
                if custom_handlers and type(current) in custom_handlers:
                    current = custom_handlers[type(current)](current)

                if value_transform:
                    current = value_transform(current)

                local_flattened[prefix] = current

        return local_flattened

    if parallel:
        chunks = [data[i::4] for i in range(4)]
        with ProcessPoolExecutor() as executor:
            results = executor.map(worker, chunks)
            for result in results:
                flattened.update(result)
    else:
        flattened = worker(data)

    if not include_long_form:
        flattened = {key.split(key_separator)[-1]: value for key, value in flattened.items()}

    if key_filter:
        filtered_flattened = {}
        for key, value in flattened.items():
            if key in key_filter:
                filtered_flattened[key] = value
        flattened = filtered_flattened

    if sort_keys:
        flattened = dict(sorted(flattened.items()))

    if schema:
        validate_schema(flattened, schema)

    if output_format == "csv":
        write_csv(flattened, delimiter)
    elif output_format == "xlsx":
        write_xlsx(flattened)

    return flattened
