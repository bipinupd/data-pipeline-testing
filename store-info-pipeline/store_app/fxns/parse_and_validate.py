import apache_beam as beam

class ParseAndValidate(beam.DoFn):
   ERR_REC="error_at_stage_ParseAndValidate"
   def process(self, element):
       try:
        data = element.decode("utf-8").split(",")
        yield (data[2], (data[0],data[3], int(data[4]),float(data[5])))
       except Exception as exception:
        json_err = {
                "payload": element,
                "error": str(exception),
                "error_step_id": self.ERR_REC
            }
        yield beam.pvalue.TaggedOutput(self.ERR_REC, json_err)

class TupToDictStoreInfo(beam.DoFn):
  def process(self, element):
    print(element)
    retval = {}
    retval["store_id"] = element[0]
    for tup in element[1]:
      products = []
      for key in tup[0]:
        product = {}
        product['product_name']= key
        product['qty']= tup[0][key]
        products.append(product)
      retval["sales"] = tup[1]
      retval["start_date"] = tup[2]
      retval["end_date"] = tup[3]
      retval["products"] = products
    yield retval