import os


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.3f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.3f%s%s" % (num, 'Yi', suffix)


def get_file_size(file_name):
    return os.path.getsize(file_name)


def get_readable_file_size(file_name):
    return sizeof_fmt(get_file_size(file_name))
