import apache_beam as beam


class AverageFn(beam.CombineFn):

    def create_accumulator(self):
        sum = 0.0
        count = 0
        accumulator = sum, count
        return accumulator

    def add_input(self, accumulator, input):
        sum, count = accumulator
        return sum + input, count + 1

    def merge_accumulators(self, accumulators):
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)

    def extract_output(self, accumulator):
        sum, count = accumulator
        if count == 0:
            return float('NaN')
        return sum / count
