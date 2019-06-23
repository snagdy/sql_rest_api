import argparse
import json
import sys
import collections
from pprint import pprint


def get_extra_keys_list(key_list, list_of_dicts):
    key_set = set()
    for _d in list_of_dicts:
        key_set = key_set.union(set(_d.keys()))
    extra_keys_list = key_set.difference(key_list)
    return list(extra_keys_list)


def update(orig_dict, new_dict):
    for key, val in new_dict.items():
        if isinstance(val, collections.Mapping):
            tmp = update(orig_dict.get(key, {}), val)
            orig_dict[key] = tmp
        elif isinstance(val, list):
            orig_dict[key] = (orig_dict.get(key, []) + val)
        else:
            orig_dict[key] = new_dict[key]
    return orig_dict


def list_to_nested_dict(input_key_list, remainder_dict):
    nested_dict = {}
    for i in range(1, len(input_key_list) + 1):
        if i == 1:
            nested_dict = {input_key_list[-i]: [remainder_dict]}
        else:
            nested_dict = {input_key_list[-i]: nested_dict}
    return nested_dict


def regroup_json_data(_json, hierarchy_list, extra_keys_list):
    for j in range(len(_json)):
        d = _json[j]
        r = {key: d[key] for key in extra_keys_list}
        try:
            if j < 1:
                _dict = list_to_nested_dict([d[key] for key in hierarchy_list], r)
            else:
                new_dict = list_to_nested_dict([d[key] for key in hierarchy_list], r)
                update(_dict, new_dict)
        except KeyError:
            _dict = {
                'status': '400',
                'message': 'A key in hierarchy does not exist in data, please pass a valid key from your JSON data.'
            }
    return json.dumps(_dict)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('args', nargs='*', help='Enter group levels in nesting order, e.g. parent child grandchild')
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    inputs = parser.parse_args()

    nesting_order = inputs.args
    input_data = inputs.infile.read()
    input_json = json.loads(input_data)

    extra_keys = get_extra_keys_list(nesting_order, input_json)
    pprint(regroup_json_data(input_json, nesting_order, extra_keys))


if __name__ == '__main__':
    main()
