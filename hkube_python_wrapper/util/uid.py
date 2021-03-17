import random
import string
def uid(length=4):
    return ''.join(random.choice(string.ascii_lowercase) for x in range(length))
