import apache_beam as beam

class CombineProductFn(beam.CombineFn):
  def create_accumulator(self):
    product_info = dict()
    total_sales = 0.0
    start_date = ""
    end_date = ""
    accumulator = product_info, total_sales, start_date, end_date
    return accumulator

  def add_input(self, accumulator, input):
    product_info, total_sales, start_date, end_date = accumulator
    print("input[0] is" ,input[0])
    print("input[1] is" ,input[1])

    if input[1] not in product_info:
      product_info[input[1]] = int(input[2])
      start_date = input[0]
      end_date = input[0]
    else:
      acc_element = product_info[input[1]]
      product_info[input[1]] =  int(acc_element)+int(input[2])
    total_sales = total_sales + float(input[3])
    start_date = min(start_date, input[0])
    end_date = max(end_date, input[0])
    return  product_info, total_sales, start_date, end_date
  def merge_accumulators(self, accumulators):
    return accumulators

  def extract_output(self, accumulator):
    return accumulator