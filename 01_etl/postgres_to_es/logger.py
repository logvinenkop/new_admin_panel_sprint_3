import logging
import sys

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter(fmt="[%(asctime)s: %(levelname)s] %(message)s"))
log.addHandler(handler)
