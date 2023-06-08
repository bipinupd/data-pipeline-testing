import apache_beam as beam


class CustomCountByKey(beam.PTransform):
    """Count as a subclass of PTransform, with an apply method."""

    def expand(self, pcoll):
        return (pcoll | 'ParWithOne' >> beam.Map(lambda v: (v, 1)) |
                beam.CombinePerKey(sum))
