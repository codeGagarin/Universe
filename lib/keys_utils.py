__all__ = ['clone']


def clone(key: dict, add_properties: dict = None):
    result = key.copy()
    add_properties = add_properties or {}
    for key, value in add_properties:
        n_val = 'never_mind_value'
        assert result.get(key, n_val) == n_val  # no overwrite key fields possible
        result[key] = value
    return result
