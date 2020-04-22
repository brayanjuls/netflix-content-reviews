from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
import kaggle


class DownloadKaggleDataSet(BaseOperator):

    @apply_defaults
    def __init__(self,
                 name,
                 destination_path,
                 unzip_file=True,
                 *args, **kwargs):
        super(DownloadKaggleDataSet, self).__init__(*args, **kwargs)
        self.name = name
        self.destination_path = destination_path
        self.unzip_file = unzip_file

    def execute(self, context):
        kaggle.api.dataset_download_files(dataset=self.name, path=self.destination_path, unzip=self.unzip_file)
