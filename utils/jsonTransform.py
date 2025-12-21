import jmespath


def parse_type(data: dict, expr: str):
    return jmespath.search(expr, data)
