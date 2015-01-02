
import hashlib

# Any values up to 160 can be used for M to define the key space.
# hashlib.sha1 hashes are 20 bytes long, hence m = 160 bits.
M = 5
RING_SIZE = 2 ** M


def hash_function(key):
    h = hashlib.sha1()
    h.update(str(key).encode())
    return int(h.hexdigest(), 16) % RING_SIZE


class NodeId(object):

    def __init__(self, ip, port, key=None):
        self.ip = ip
        self.port = port

        if key is not None:
            self.key = key
        else:
            self.key = hash_function("%s:%s" % (ip, port))

    def __str__(self):
        return "[%s]" % self.key

    def __eq__(self, other):
        return self.ip == other.ip and self.port == other.port

    def __ne__(self, other):
        return not self.__eq__(other)



class Interval(object):

    def __init__(self, start, end, circle_size=RING_SIZE, closed_on_left=True, closed_on_right=True):
        self.start = start
        self.end = end
        self.circle_size = circle_size

        self.closed_on_left = closed_on_left
        self.closed_on_right = closed_on_right

    def __contains__(self, value):
        if self.start == self.end:
            return True

        if self.start > self.end:
            # cyclic interval
            in_before_cycle = value in Interval(start=self.start,
                                     end=self.circle_size,
                                     closed_on_left=self.closed_on_left, 
                                     closed_on_right=True)

            if self.closed_on_right:
                in_after_cycle = self.start + value <= self.start + self.end
            else:
                in_after_cycle = self.start + value < self.start + self.end

            return in_before_cycle or in_after_cycle
        else:

            if self.closed_on_left:
                part_1 = self.start <= value
            else:
                part_1 = self.start < value

            if self.closed_on_right:
                part_2 = self.end >= value
            else:
                part_2 = self.end > value

            return part_1 and part_2