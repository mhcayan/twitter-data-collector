from Constants import PATTERN

def get_status_id_from_status_url(url):
    return PATTERN.STATUS_URL.value.findall(url)[0]

def get_screen_name_from_profile_url(url):
    return PATTERN.PROFILE_URL.value.findall(url)[0]