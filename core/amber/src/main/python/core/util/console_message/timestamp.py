import datetime
import tzlocal


def current_time_in_local_timezone():
    # Get the system's local timezone
    local_timezone = tzlocal.get_localzone()

    # Get the current time in the local timezone
    local_time = datetime.datetime.now(local_timezone)

    return local_time
