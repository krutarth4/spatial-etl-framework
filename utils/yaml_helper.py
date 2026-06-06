def deep_get(dictionary, *keys, default=None):
    d = dictionary
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k, default)
        else:
            return default
    return d
