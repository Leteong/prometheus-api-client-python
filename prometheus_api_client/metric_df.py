from pandas import DataFrame


class MetricDataFrame(DataFrame):
    """
    Subclass to format and represent Prometheus query response as pandas.DataFrame.
    Assumes response is either a json or sequence of jsons.
    This is different than passing raw list of jsons to pandas.DataFrame in that it
    unpacks metric label values, extracts (first or last) timestamp-value pair (if
    multiple pairs are retuned), and concats them before passing to the pandas
    DataFrame constructor.
    """

    def __init__(self, metric_name: str, data):
        rows = list()

        for metric_values in data:
            rows += MetricDataFrame._parse_metric_values(metric_values)

        # init df normally now
        super(MetricDataFrame, self).__init__(data=rows, columns=['pod', 'timestamp', metric_name])

    @property
    def _constructor_expanddim(self):
        raise NotImplementedError('Not supported for DataFrames!')

    @staticmethod
    def _parse_metric_values(metric_values: dict):
        pod_name = metric_values['metric']['pod']

        for value in metric_values['values']:
            yield pod_name, value[0], value[1]
