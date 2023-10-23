import re


def instance_to_array(instance):
    return [value for key, value in instance.__dict__.items()]

def check_patterns_occurrence(arr, patterns, exact = False):
    for pattern in patterns:
        pattern_regex = re.compile(pattern, re.IGNORECASE)
        for item in arr:
            if exact == False:
                if pattern_regex.search(str(item)):
                    return 1
            else:
                if pattern_regex.fullmatch(str(item)):
                    return 1
    return 0