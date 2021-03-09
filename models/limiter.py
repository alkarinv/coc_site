

class RateLimiter:
    def __init__(self, rate :float, capacity: int):
        """[summary]

        Args:
            rate (float): Number of tokens per second to add to the bucket
            capacity (int): Maximum tokens bucket can hold
        """