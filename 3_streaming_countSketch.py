from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import numpy as np

# Constants
THRESHOLD = 10000000  # After how many items should we stop?
P = 8191  # Prime number

class CountSketch:
    def __init__(self, d, w):
        self.d = d
        self.w = w
        self.count_sketch = np.zeros((self.d, self.w), dtype=np.int64)
        self.ha = np.random.randint(1, P, size=self.d)
        self.hb = np.random.randint(0, P, size=self.d)

    def _get_count_sketch(self, index):
        return self.count_sketch[index]

    def update(self, elem, count):
        for j in range(self.d):
            hash_val = self._hash_function(self.ha[j], self.hb[j], elem)
            g        = self._generate_sign(elem)
            self.count_sketch[j][hash_val] += g * count

    def estimate_item_frequency(self, elem):
        f_tilde = []
        for j in range(self.d):
            hash_val = self._hash_function(self.ha[j], self.hb[j], elem)
            g        = self._generate_sign(elem)
            f_tilde.append(self._get_count_sketch(j)[hash_val] * g)
        return np.median(f_tilde)

    def estimate_F2(self):
        return np.median([sum(self._get_count_sketch(i) ** 2) for i in range(D)])

    def _hash_function(self, a, b, x):
        return ((a * x + b) % P) % self.w

    @staticmethod
    def _generate_sign(x):
        random_generator = np.random.default_rng(x)
        return random_generator.choice([-1, 1])

# Operations to perform after receiving an RDD 'batch'
def process_batch(batch):
    global streamLength, true_freq_histogram, count_sketch, D, W, left, right

    if streamLength[0] >= THRESHOLD:
        return

    batch_size = batch.count()
    if batch_size > 0:
        streamLength[0] += batch_size
        batch_item_freq = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda i1, i2: i1 + i2).collectAsMap()

        for key, value in batch_item_freq.items():
            if key in range(left, right+1):
                if key not in true_freq_histogram:
                    true_freq_histogram[key] = value
                else:
                    true_freq_histogram[key] += value
                count_sketch.update(key, value)

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()

def print_statistics(portNumber, length_StreamInterval, distinct_StreamInterval):
    print(f'D = {D}, W = {W}, [left,right] = [{left},{right}], K = {K}, Port = {portNumber}')
    print(f'Total number of items = {streamLength[0]}')
    print(f'Total number of items in [{left}, {right}] = {length_StreamInterval}')
    print(f'Number of distinct items in [{left}, {right}] = {distinct_StreamInterval}')

    top_K_freq = sorted(true_freq_histogram.items(), key=lambda x: x[1], reverse=True)[:K]

    if K <= 20:
        for i, freq in top_K_freq:
            print(f'Item {i} Freq = {freq} Est. Freq = {approximate_freq[i]}')

    avg_error = 0.0
    for i, freq in top_K_freq:
        avg_error += abs(freq - approximate_freq[i]) / freq
    avg_error /= K

    print(f'Avg err for top {K} = {avg_error:.4f}')
    print(f'F2 {F2:.4f} F2 Estimate {F2_aprox:.4f}')


if __name__ == '__main__':
    assert len(sys.argv) == 7, "USAGE: D W left right K portExp"

    conf = SparkConf().setAppName("Streaming_CountSketch_HW3")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    stopping_condition = threading.Event()

    # INPUT READING
    D       = int(sys.argv[1])
    W       = int(sys.argv[2])
    left    = int(sys.argv[3])
    right   = int(sys.argv[4])
    K       = int(sys.argv[5])
    portExp = int(sys.argv[6])

    streamLength        = [0]
    true_freq_histogram = {}
    count_sketch        = CountSketch(D, W)

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda batch: process_batch(batch))

    # MANAGING STREAMING SPARK CONTEXT
    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, True)

    # COMPUTE AND PRINT FINAL STATISTICS
    approximate_freq = {}
    for item in true_freq_histogram.keys():
        approximate_freq[item] = count_sketch.estimate_item_frequency(item)

    sigmaR_length   = sum(val for val in true_freq_histogram.values())
    sigmaR_distinct = len(true_freq_histogram)
    F2              = sum(val ** 2 for val in true_freq_histogram.values()) / sigmaR_length ** 2
    F2_aprox        = count_sketch.estimate_F2() / sigmaR_length ** 2

    print_statistics(portExp, sigmaR_length, sigmaR_distinct)

    sc.stop()
