import re


def change_file_extension(filename: str, new_extension: str) -> str:
    if not bool(re.match(r'\.', new_extension)):
        new_extension = '.' + new_extension
    extension_pattern = r'\.\w+$'
    if not bool(re.match(extension_pattern, new_extension)):
        raise ValueError(f'The specified extension {new_extension} is invalid')
    new_filename = re.sub(extension_pattern, new_extension, filename)
    return new_filename
