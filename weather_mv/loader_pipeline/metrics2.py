import time
import copy
import inspect
from functools import wraps
import apache_beam as beam
from apache_beam import DoFn, Pipeline, PCollection
from apache_beam.options.pipeline_options import PipelineOptions


def timeit(func_name: str, first_timer: bool = False):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            time_dict = {}

            # Only the first timer wrapper will have no time_dict.
            # All subsequent wrappers can extract out the dict.
            if not first_timer:
                # args 0 would be a tuple.
                if len(args[0]) == 1:
                    raise ValueError('time_dict not found.')

                element, time_dict = args[0]
                args = (element,) + args[1:]

                if not isinstance(time_dict, dict):
                    raise ValueError('time_dict not found.')

            # If the function is a generator, yield the output
            # othewise return it.
            if inspect.isgeneratorfunction(func):
                for result in func(self, *args, **kwargs):
                    end_time = time.time()
                    processing_time = end_time - start_time
                    new_time_dict = copy.deepcopy(time_dict)
                    new_time_dict[func_name] = processing_time
                    yield result, new_time_dict
            else:
                result = func(self, *args, **kwargs)
                end_time = time.time()
                processing_time = end_time - start_time
                new_time_dict = copy.deepcopy(time_dict)
                new_time_dict[func_name] = processing_time
                return result, new_time_dict

        return wrapper
    return decorator

class YourExistingDoFn(beam.DoFn):
    @timeit("simple dofn", first_timer=True)
    def process(self, element):
        yield element * 2

class YourExistingDoFn2(beam.DoFn):
    @timeit("simple dofn 2")
    def process(self, element):
        yield element * 3

# Example usage within a pipeline
def main():
    options = PipelineOptions()
    with Pipeline(options=options) as p:

        _ = (
            p
            | 'Create' >> beam.Create([1, 2, 3, 4, 5])
            | 'First ParDo' >> beam.ParDo(YourExistingDoFn())
            | 'Second ParDo' >> beam.ParDo(YourExistingDoFn2())
            | 'Print Result' >> beam.Map(print)
        )

if __name__ == '__main__':
    main()